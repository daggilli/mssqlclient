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
const std::string testqstr = "SELECT Id, Value FROM dbo.Test ORDER BY Id;";
const std::string eventqstr = "SELECT Id, Name, EventTime FROM dbo.Events ORDER BY EventTime;";
std::string dateParse(const DBDATEREC &dateRecord);
std::uintmax_t loadFile(const char *const name, std::string &fileStr);
std::size_t loadFileByLines(const char *const name, std::vector<std::string> &lineVec);
MSSQLClient::DatabaseConfig getDatabaseConfiguration(const std::string &configFile);

int msgHandler(DBPROCESS *dbproc, DBINT msgno, int msgstate, int severity, char *msgtext, char *srvname, char *procname,
               int line);
int errHandler(DBPROCESS *dbproc, int severity, int dberr, int oserr, char *dberrstr, char *oserrstr);

int main(int argc, char *argv[]) {
  std::string configStr;

  MSSQLClient::Connection connection(getDatabaseConfiguration("./dbconfig.json"), msgHandler, errHandler);

  MSSQLClient::RecordSet result = connection.query(testqstr);

  std::cout << "ROWS " << result.size() << '\n';

  for (auto &r : result) {
    std::cout << r[0].get<int>() << ": " << r[1].get<std::string>() << "\n";
  }

  result = connection.query(eventqstr);

  DBDATEREC dateRecord;

  for (auto &r : result) {
    dbdatecrack(nullptr, &dateRecord, const_cast<DBDATETIME *>(&(r[2].get<DBDATETIME>())));
    std::cout << r[0].get<int>() << ": " << r[1].get<std::string>() << " | " << dateParse(dateRecord) << "\n";
  }

  const std::size_t PNBUFSIZE = 100;

  int inputParameter = 1000;
  int maxEvent = 0;
  auto procNameBuf = std::make_unique<char[]>(PNBUFSIZE + 1);

  MSSQLClient::ParameterList params = {{"@InputParameter", SYBINT4, -1, false, reinterpret_cast<BYTE *>(&inputParameter)},
                                       {"@MaxEvent", SYBINT4, -1, true, reinterpret_cast<BYTE *>(&maxEvent)},
                                       {"@ProcName", SYBVARCHAR, PNBUFSIZE, true, reinterpret_cast<BYTE *>(procNameBuf.get())}};

  {
    std::cout << "NEST\n";

    MSSQLClient::Connection conn(getDatabaseConfiguration("./db2config.json"), msgHandler, errHandler);

    std::vector<std::string> sqlCmds;

    loadFileByLines("./commands.sql", sqlCmds);

    MSSQLClient::RecordSet rs = conn.query(sqlCmds[0]);

    std::cout << rs.size() << "\n";

    std::cout << rs[0][0].get<int>() << "\n";
  }

  MSSQLClient::ProcedureResult procResult = connection.procedure("TestProcedure", params, {NTBSTRINGBIND});

  for (auto &[k, v] : procResult.returnValues) {
    std::cout << k << ": " << (std::holds_alternative<int>(v) ? std::to_string(std::get<int>(v)) : std::get<std::string>(v))
              << '\n';
  }

  for (auto &r : procResult.recordSet) {
    std::cout << r[0].get<std::string>() << '\n';
  }

  if (procResult.procedureReturnValue.has_value()) {
    std::cout << "Procedure returned " << procResult.procedureReturnValue.value() << '\n';
  }

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
  fs::path filepath(fs::absolute(fs::path(name)));

  std::uintmax_t fsize;

  if (fs::exists(filepath)) {
    fsize = fs::file_size(filepath);
  } else {
    std::throw_with_nested(std::invalid_argument("File not found: " + filepath.string()));
  }

  std::ifstream infile;
  infile.exceptions(std::ifstream::failbit | std::ifstream::badbit);
  try {
    infile.open(filepath, std::ios::in | std::ifstream::binary);
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

std::size_t loadFileByLines(const char *const name, std::vector<std::string> &lineVec) {
  std::string sqlStr;

  loadFile(name, sqlStr);

  std::istringstream istr(sqlStr);

  std::string line;
  while (std::getline(istr, line)) {
    lineVec.emplace_back(std::move(line));
  }

  return lineVec.size();
}

MSSQLClient::DatabaseConfig getDatabaseConfiguration(const std::string &configFile) {
  std::string configStr;

  loadFile(configFile.c_str(), configStr);
  auto dbConfig = json::parse(configStr);

  return {dbConfig["host"], dbConfig["username"], dbConfig["password"], dbConfig["database"]};
}
