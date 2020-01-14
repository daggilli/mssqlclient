#ifndef __MSSQLCLIENT_H__
#define __MSSQLCLIENT_H__
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <functional>
#include <iomanip>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <variant>
#include <vector>

#include <sybdb.h>
#include <sybfront.h>

namespace MSSQLClient {
  struct DatabaseConfig {
    std::string host;
    std::string username;
    std::string password;
    std::string database;
  };

  constexpr std::size_t NUMERICSIZE = sizeof(DBNUMERIC);
  constexpr std::size_t NUMERICBYTESSTART = 2;
  constexpr std::size_t NUMERICBYTESEND = 17 + 2;

  using TypeValue = std::variant<int8_t, int16_t, int32_t, int64_t, uint8_t, uint16_t, uint32_t, uint64_t, float, double,
                                 std::string, DBDATETIME, DBMONEY, DBNUMERIC>;
  using ItemValue = std::optional<TypeValue>;

  class Item {
   public:
    Item(const int type, const ItemValue &value) : tp(type), val(value) {}
    const bool isNull() { return val.has_value(); }
    const TypeValue &value() const { return val.value(); }
    template <typename T>
    const T &get() const {
      return std::get<T>(val.value());
    }

   private:
    int tp;
    ItemValue val;
  };

  using Record = std::vector<Item>;
  using RecordSet = std::vector<Record>;

  using Param = struct {
    std::string name;
    int type;
    ssize_t maxLength;
    bool output;
    BYTE *valueBuffer;
  };

  using ParameterList = std::list<Param>;
  using ReturnValueMap = std::map<std::string, TypeValue>;

  using ProcedureResult = struct {
    RecordSet recordSet;
    ReturnValueMap returnValues;
    std::optional<DBINT> procedureReturnValue;
  };

  namespace {
    const std::unordered_map<int, int> typeMap = {{SYBINT4, INTBIND},  {SYBCHAR, NTBSTRINGBIND}, {SYBDATETIME, DATETIMEBIND},
                                                  {SYBFLT8, FLT8BIND}, {SYBMONEY, MONEYBIND},    {SYBNUMERIC, NUMERICBIND}};
  }

  class Column {
   public:
    Column(DBPROCESS *dbproc, const int col, const int colType)
        : nm(dbcolname(dbproc, col)), tp(dbcoltype(dbproc, col)), dtp(colType), sz(dbcollen(dbproc, col)) {
      if (dtp == -1) {
        auto mappedType = typeMap.find(tp);
        if (mappedType == typeMap.end()) {
          throw(std::runtime_error("Could not infer column type " + std::to_string(tp)));
        }
        dtp = mappedType->second;
      }

      if (dtp == NUMERICBIND || dtp == DECIMALBIND) {
        sz = NUMERICSIZE;
      }

      buf = std::make_unique<char[]>(sz + 1);

      if (dbbind(dbproc, col, dtp, sz + 1, reinterpret_cast<BYTE *>(buf.get())) == FAIL) {
        throw(std::runtime_error("dbbind() failed"));
      }

      if (dbnullbind(dbproc, col, &st) == FAIL) {
        throw(std::runtime_error("dbnullbind() failed"));
      }
    };
    Column() = delete;
    Column(const Column &col) = delete;
    Column(Column &&col) noexcept
        : nm(std::move(col.nm)),
          buf(std::move(col.buf)),
          tp(std::exchange(col.tp, 0)),
          sz(std::exchange(col.sz, 0)),
          st(std::exchange(col.st, 0)) {}
    ~Column() {}

    const std::string &name() const { return nm; }
    const int type() const { return tp; }
    const int dataType() const { return dtp; }
    const int size() const { return sz; }
    const int status() const { return st; }
    const char *const buffer() const { return buf.get(); }

   private:
    std::string nm;
    std::unique_ptr<char[]> buf;
    int tp;
    int dtp;
    int sz;
    int st;
  };  // class Column

  using ColumnSet = typename std::vector<Column>;

  using MessageHandler = std::add_pointer<int(DBPROCESS *, DBINT, int, int, char *, char *, char *, int)>::type;
  using ErrorHandler = std::add_pointer<int(DBPROCESS *, int, int, int, char *, char *)>::type;

  namespace {
    template <typename T>
    inline T const getItem(const char *const buffer) {
      return *(reinterpret_cast<const T *>(buffer));
    }

    template <>
    inline uint8_t const getItem(const char *const buffer) {
      return static_cast<uint8_t>(buffer[0] & 0xFF);
    }

    template <>
    inline std::string const getItem(const char *const buffer) {
      return std::string(buffer);
    }

    template <>
    inline DBNUMERIC const getItem(const char *const buffer) {
      DBNUMERIC numer = {static_cast<uint8_t>(buffer[0]), static_cast<uint8_t>(buffer[1])};
      std::copy(buffer + NUMERICBYTESSTART, buffer + NUMERICBYTESEND, numer.array);
      return numer;
    }

    template <typename T>
    inline std::string const getItem(const char *const buffer, const std::size_t len) {
      return std::string(buffer, len);
    }
  }  // namespace

  inline std::string datetimeString(const DBDATETIME &dt) {
    DBDATEREC dateRecord;
    dbdatecrack(nullptr, &dateRecord, const_cast<DBDATETIME *>(&dt));
    std::ostringstream dateStr;

    dateStr << dateRecord.dateyear << "-" << std::setw(2) << std::setfill('0') << dateRecord.datemonth + 1 << "-" << std::setw(2)
            << std::setfill('0') << dateRecord.datedmonth << " " << dateRecord.datehour << ":" << std::setw(2)
            << std::setfill('0') << dateRecord.dateminute << ":" << std::setw(2) << std::setfill('0') << dateRecord.datesecond;

    return dateStr.str();
  }

  class Connection {
   public:
    Connection() = delete;
    Connection(const DatabaseConfig &config, MessageHandler msgHandler = nullptr, ErrorHandler errHandler = nullptr)
        : dbproc(nullptr) {
      try {
        if (!refCnt++ && dbinit() == FAIL) {
          throw(std::runtime_error("dbinit() failed'"));
        }

        if (dbsetversion(DBVERSION_100) == FAIL) {
          throw(std::runtime_error("dbsetversion() failed'"));
        }

        if (errHandler != nullptr) {
          dberrhandle(errHandler);
        }
        if (msgHandler != nullptr) {
          dbmsghandle(msgHandler);
        }

        std::unique_ptr<LOGINREC, std::function<void(LOGINREC *)>> login(dblogin(), [](LOGINREC *login) {
          if (login != nullptr) {
            dbloginfree(login);
          }
        });
        LOGINREC *loginrec;
        if ((loginrec = login.get()) == nullptr) {
          throw(std::runtime_error("dblogin() failed"));
        }

        DBSETLUSER(loginrec, config.username.c_str());
        DBSETLPWD(loginrec, config.password.c_str());

        RETCODE erc;

        if ((dbproc = dbopen(loginrec, config.host.c_str())) == NULL) {
          throw(std::runtime_error("dbopen() failed"));
        }

        if ((erc = dbuse(dbproc, config.database.c_str())) == FAIL) {
          throw(std::runtime_error("dbuse() failed"));
        }
      } catch (...) {
        std::throw_with_nested(std::runtime_error("Connection constrcutor failed"));
      }
    }
    Connection(const Connection &conn) = delete;
    ~Connection() {
      close();
      if (!--refCnt) dbexit();
    }

    RecordSet query(const std::string &queryString, const std::vector<int> &expectedTypes = {}) {
      return query(queryString.c_str(), expectedTypes);
    }

    RecordSet query(const char *queryString, const std::vector<int> &expectedTypes = {}) {
      try {
        if (dbproc == nullptr) {
          throw(std::runtime_error("Datanase process invalid"));
        }

        if (dbcmd(dbproc, queryString) == FAIL) {
          throw(std::runtime_error("dbcmd() failed"));
        }

        if (dbsqlexec(dbproc) == FAIL) {
          throw(std::runtime_error("dbsqlexec() failed"));
        }

        return getResultRows(expectedTypes);
      } catch (...) {
        std::throw_with_nested(std::runtime_error("query() failed"));
      }
    }

    ProcedureResult procedure(const std::string &procedureName, const ParameterList &params,
                              const std::vector<int> &expectedTypes = {}) {
      try {
        if (dbrpcinit(dbproc, "TestProcedure", static_cast<DBSMALLINT>(0)) == FAIL) {
          throw(std::runtime_error("dbprcinit() failed"));
        }

        for (auto &p : params) {
          if (addParameter(p) == FAIL) {
            throw(std::runtime_error("addParameter() failed.\n"));
          }
        }

        if (dbrpcsend(dbproc) == FAIL) {
          throw(std::runtime_error("dbrpcsend() failed"));
        }

        if (dbsqlok(dbproc) == FAIL) {
          throw(std::runtime_error("dbsqlok failed.\n"));
        }

        ProcedureResult procResult = {getResultRows(expectedTypes)};

        getReturnValues(procResult);

        return procResult;
      } catch (...) {
        std::throw_with_nested(std::runtime_error("procedure() failed"));
      }
    }

    void close() {
      if (dbproc != nullptr) {
        dbclose(dbproc);
        dbproc = nullptr;
      }
    }

    RETCODE cancel() {
      RETCODE returnCode = SUCCEED;
      try {
        if (dbproc == nullptr) {
          throw(std::runtime_error("Datanase process invalid"));
        }

        returnCode = dbcancel(dbproc);
      } catch (...) {
        std::throw_with_nested(std::runtime_error("procedure() failed"));
      }

      return returnCode;
    }

    RETCODE cancelQuery() {
      RETCODE returnCode = SUCCEED;
      try {
        if (dbproc == nullptr) {
          throw(std::runtime_error("Datanase process invalid"));
        }

        returnCode = dbcanquery(dbproc);
      } catch (...) {
        std::throw_with_nested(std::runtime_error("procedure() failed"));
      }

      return returnCode;
    }

    RETCODE setOption(const int option, char *const param = nullptr, const int paramLen = -1) {
      RETCODE returnCode = SUCCEED;
      try {
        if (dbproc == nullptr) {
          throw(std::runtime_error("Datanase process invalid"));
        }

        returnCode = dbsetopt(dbproc, option, param, paramLen);
      } catch (...) {
        std::throw_with_nested(std::runtime_error("procedure() failed"));
      }

      return returnCode;
    }

    RETCODE clearOption(const int option, char *const param = nullptr) {
      RETCODE returnCode = SUCCEED;
      try {
        if (dbproc == nullptr) {
          throw(std::runtime_error("Datanase process invalid"));
        }

        returnCode = dbclropt(dbproc, option, param);
      } catch (...) {
        std::throw_with_nested(std::runtime_error("procedure() failed"));
      }

      return returnCode;
    }

    static const uint32_t refCount() { return Connection::refCnt.load(); }

   private:
    RecordSet getResultRows(const std::vector<int> &expectedTypes) {
      try {
        RecordSet result;
        RETCODE erc;

        while ((erc = dbresults(dbproc)) != NO_MORE_RESULTS) {
          if (erc == FAIL) {
            throw(std::runtime_error("dbresults() failed"));
          }

          bool useExpectedTypes = expectedTypes.size() != 0;
          std::size_t ncols;

          ncols = static_cast<std::size_t>(dbnumcols(dbproc));

          if (useExpectedTypes && ncols != expectedTypes.size()) {
            std::ostringstream err;
            err << "Column number mismatch: expected " << expectedTypes.size() << ", got " << ncols;

            throw(std::runtime_error(err.str()));
          }

          int rowCode;

          ColumnSet colSet;

          colSet.reserve(ncols);

          for (std::size_t c = 0; c < ncols; c++) {
            colSet.emplace_back(dbproc, c + 1, useExpectedTypes ? expectedTypes[c] : -1);
          }

          while ((rowCode = dbnextrow(dbproc)) != NO_MORE_ROWS) {
            switch (rowCode) {
              case REG_ROW: {
                Record row;
                for (auto &c : colSet) {
                  const char *const buf = c.status() == -1 ? nullptr : c.buffer();
                  ItemValue it;
                  if (buf) {
                    switch (c.dataType()) {
                      case INTBIND: {
                        it = getItem<int32_t>(buf);
                        break;
                      }

                      case TINYBIND: {
                        it = getItem<uint8_t>(buf);
                        break;
                      }

                      case SMALLBIND: {
                        it = getItem<int16_t>(buf);
                        break;
                      }

                      case REALBIND: {
                        it = getItem<float>(buf);
                        break;
                      }

                      case FLT8BIND: {
                        it = getItem<double>(buf);
                        break;
                      }

                      case NTBSTRINGBIND: {
                        it = getItem<std::string>(buf);
                        break;
                      }

                      case DATETIMEBIND: {
                        it = getItem<DBDATETIME>(buf);
                        break;
                      }

                      case MONEYBIND: {
                        it = getItem<DBMONEY>(buf);
                        break;
                      }

                      case NUMERICBIND: {
                        it = getItem<DBNUMERIC>(buf);
                        break;
                      }
                    }
                  }
                  row.emplace_back(c.type(), it);
                }
                result.emplace_back(row);
                break;
              }

              case BUF_FULL: {
                throw(std::runtime_error("BUF_FULL in dbnextrow()"));
              }

              case FAIL: {
                throw(std::runtime_error("dbresults() failed in dbnextrow()"));
              }

              default: {
                std::cerr << "Ignore row code " << rowCode << '\n';
                break;
              }
            }
          }
        }
        return result;
      } catch (...) {
        std::throw_with_nested(std::runtime_error("getResultRows() failed"));
      }
    }

    int getReturnValues(ProcedureResult &procResult) {
      int numrets = dbnumrets(dbproc);

      for (auto i = 1; i <= numrets; i++) {
        auto retType = dbrettype(dbproc, i);
        std::string returnName(dbretname(dbproc, i));
        TypeValue it;

        const char *const returnDataPtr = reinterpret_cast<const char *>(dbretdata(dbproc, i));

        switch (retType) {
          case SYBINT1: {
            it = getItem<int8_t>(returnDataPtr);
            break;
          }

          case SYBINT2: {
            it = getItem<int16_t>(returnDataPtr);
            break;
          }

          case SYBINT4: {
            it = getItem<int32_t>(returnDataPtr);
            break;
          }

          case SYBINT8: {
            it = getItem<int64_t>(returnDataPtr);
            break;
          }

          case SYBFLT8: {
            it = getItem<double>(returnDataPtr);
            break;
          }

          case SYBVARCHAR: {
            it = getItem<std::string>(returnDataPtr, dbretlen(dbproc, i));
            break;
          }

          case SYBDATETIME: {
            it = getItem<DBDATETIME>(returnDataPtr);
            break;
          }

          case SYBMONEY: {
            it = getItem<DBMONEY>(returnDataPtr);
            break;
          }

          case SYBNUMERIC: {
            it = getItem<DBNUMERIC>(returnDataPtr);
            break;
          }
        }

        procResult.returnValues[returnName] = std::move(it);
      }

      procResult.procedureReturnValue = std::nullopt;

      if (dbhasretstat(dbproc) == TRUE) {
        procResult.procedureReturnValue = dbretstatus(dbproc);
      }

      return numrets;
    }

    RETCODE addParameter(const Param &p) {
      DBINT maxLen = -1;
      DBINT dataLen = -1;

      if (p.output) {
        maxLen = p.maxLength;
        if (p.type == SYBVARCHAR) dataLen = maxLen;
      } else {
        if (p.type == SYBVARCHAR) {
          maxLen = p.maxLength;
        }
      }
      return dbrpcparam(dbproc, p.name.c_str(), static_cast<BYTE>(p.output ? DBRPCRETURN : 0), p.type, maxLen, dataLen,
                        p.valueBuffer);
    }

    DBPROCESS *dbproc;
    inline static std::atomic_uint32_t refCnt = 0;
  };  // namespace MSSQLClient
}  // namespace MSSQLClient

#endif
