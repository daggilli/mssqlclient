// g++ -O4 -std=gnu++17 -Wall -Wunused mssqltest.cpp -lsybdb -lstdc++fs -o mssqltest
#include <unistd.h>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <nlohmann/json.hpp>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include <sybdb.h>
#include <sybfront.h>

#include "mssqlClient.h"

namespace fs = std::filesystem;
using json = nlohmann::json;

const std::string qstr = "SELECT CAST(1.9 AS FLOAT) AS flt, 'yay' as yay, CURRENT_TIMESTAMP AS n;";
const std::string testqstr = "SELECT Id, Value FROM Test ORDER BY Id;";

std::string dateParse(const DBDATEREC &dateRecord);
std::uintmax_t loadFile(const char *const name, std::string &fileStr);
MSSQLClient::DatabaseConfig getDatabaseConfiguration(const std::string &configFile);

int msgHandler(DBPROCESS *dbproc, DBINT msgno, int msgstate, int severity, char *msgtext, char *srvname, char *procname,
               int line);
int errHandler(DBPROCESS *dbproc, int severity, int dberr, int oserr, char *dberrstr, char *oserrstr);

int main(int argc, char *argv[]) {
  std::string configStr;

  loadFile("./dbconfig.json", configStr);
  std::cout << configStr << "\n";
  auto dbConfig = json::parse(configStr);
  std::cout << dbConfig << "\n";

  MSSQLClient::Connection connection(getDatabaseConfiguration("./dbconfig.json"), msgHandler, errHandler);

  MSSQLClient::RecordSet result = connection.query(testqstr);

  std::cout << "ROWS " << result.size() << '\n';

  // DBDATEREC dateRecord;

  for (auto &r : result) {
    std::cout << r[0].get<int>() << ": " << r[1].get<std::string>() << "\n";
    /* dbdatecrack(nullptr, &dateRecord, const_cast<DBDATETIME *>(&(r[2].get<DBDATETIME>())));
    std::cout << r[0].get<double>() << " " << r[1].get<std::string>() << " " << dateParse(dateRecord) << "\n";*/
  }

#if 0
  result = connection.query(statsqstr, {INTBIND, NTBSTRINGBIND, DATETIMEBIND});

  std::cout << "ROWS " << result.size() << '\n';

  for (auto &r : result) {
    dbdatecrack(nullptr, &dateRecord, const_cast<DBDATETIME *>(&(r[2].get<DBDATETIME>())));
    std::cout << r[0].get<int>() << " " << r[1].get<std::string>() << " " << dateParse(dateRecord) << "\n";
  }

  const std::size_t PNBUFSIZE = 100;

  int timeInterval = 10;
  int evtCnt = 0;
  auto procNameBuf = std::make_unique<char[]>(PNBUFSIZE + 1);

  MSSQLClient::ParameterList params = {{"@timeInterval", SYBINT4, -1, false, reinterpret_cast<BYTE *>(&timeInterval)},
                                       {"@EventCount", SYBINT4, -1, true, reinterpret_cast<BYTE *>(&evtCnt)},
                                       {"@ProcName", SYBVARCHAR, PNBUFSIZE, true, reinterpret_cast<BYTE *>(procNameBuf.get())}};

  MSSQLClient::ProcedureResult procResult = connection.procedure("TestProcedure", params, {NTBSTRINGBIND});

  for (auto &[k, v] : procResult.returnValues) {
    std::cout << k << ": " << (std::holds_alternative<int>(v) ? std::to_string(std::get<int>(v)) : std::get<std::string>(v))
              << '\n';
  }

  for (auto &r : procResult.recordSet) {
    std::cout << r[0].get<std::string>() << '\n';
  }
#endif

  return 0;
}

int msgHandler(DBPROCESS *dbproc, DBINT msgno, int msgstate, int severity, char *msgtext, char *srvname, char *procname,
               int line) {
  enum { changed_database = 5701, changed_language = 5703 };

  if (msgno == changed_database || msgno == changed_language) return 0;

  if (msgno > 0) {
    fprintf(stderr, "Msg %ld, Level %d, State %d\n", (long)msgno, severity, msgstate);

    if (strlen(srvname) > 0) fprintf(stderr, "Server '%s', ", srvname);
    if (strlen(procname) > 0) fprintf(stderr, "Procedure '%s', ", procname);
    if (line > 0) fprintf(stderr, "Line %d", line);

    fprintf(stderr, "\n\t");
  }
  fprintf(stderr, "%s\n", msgtext);

  if (severity > 10) {
    fprintf(stderr, "%s: error: severity %d > 10, exiting\n", "APP", severity);
    exit(severity);
  }

  return 0;
}

int errHandler(DBPROCESS *dbproc, int severity, int dberr, int oserr, char *dberrstr, char *oserrstr) {
  if (dberr) {
    fprintf(stderr, "%s: Msg %d, Level %d\n", "APP", dberr, severity);
    fprintf(stderr, "%s\n\n", dberrstr);
  } else {
    fprintf(stderr, "%s: DB-LIBRARY error:\n\t", "APP");
    fprintf(stderr, "%s\n", dberrstr);
  }

  return INT_CANCEL;
}

std::string dateParse(const DBDATEREC &dateRecord) {
  std::ostringstream dateStr;

  dateStr << dateRecord.dateyear << "-" << std::setw(2) << std::setfill('0') << dateRecord.datemonth + 1 << "-" << std::setw(2)
          << std::setfill('0') << dateRecord.datedmonth << " " << dateRecord.datehour << ":" << std::setw(2) << std::setfill('0')
          << dateRecord.dateminute << ":" << std::setw(2) << std::setfill('0') << dateRecord.datesecond;

  return dateStr.str();
}

std::uintmax_t loadFile(const char *const name, std::string &fileStr) {
  std::cout << "Loading " << name << "\n";
  fs::path filepath(fs::absolute(fs::path(name)));

  std::uintmax_t fsize;

  if (fs::exists(filepath)) {
    fsize = fs::file_size(filepath);
  } else {
    std::throw_with_nested(std::invalid_argument("File not found: " + filepath.string()));
  }

  std::cout << "Loading " << filepath.string() << "\n";
  std::ifstream infile;
  infile.exceptions(std::ifstream::failbit | std::ifstream::badbit);
  try {
    infile.open(filepath.c_str(), std::ios::in | std::ifstream::binary);
  } catch (...) {
    std::throw_with_nested(std::runtime_error("Can't open input file " + filepath.string()));
  }

  try {
    fileStr.resize(fsize);
  } catch (...) {
    std::stringstream err;
    err << "Can't reserve " << fsize << " bytes";
    std::throw_with_nested(std::runtime_error(err.str()));
  }

  infile.read(fileStr.data(), fsize);
  infile.close();

  return fsize;
}

MSSQLClient::DatabaseConfig getDatabaseConfiguration(const std::string &configFile) {
  std::string configStr;

  loadFile(configFile.c_str(), configStr);
  auto dbConfig = json::parse(configStr);

  return {dbConfig["host"], dbConfig["username"], dbConfig["password"], dbConfig["database"]};
}
