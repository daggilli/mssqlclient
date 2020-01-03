#ifndef __MSSQLCLIENT_H__
#define __MSSQLCLIENT_H__
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

  using TypeValue = std::variant<int, uint8_t, uint16_t, float, double, std::string, DBDATETIME>;
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
    const std::unordered_map<int, int> typeMap = {
        {SYBINT4, INTBIND}, {SYBCHAR, NTBSTRINGBIND}, {SYBDATETIME, DATETIMEBIND}, {SYBFLT8, FLT8BIND}};
  }

  class Column {
   public:
    Column(DBPROCESS *dbproc, const int col, const int colType)
        : nm(dbcolname(dbproc, col)), tp(dbcoltype(dbproc, col)), dtp(colType), sz(dbcollen(dbproc, col)) {
      buf = std::make_unique<char[]>(sz + 1);

      if (dtp == -1) {
        auto mappedType = typeMap.find(tp);
        if (mappedType == typeMap.end()) {
          throw(std::runtime_error("Could not infer column type " + std::to_string(tp)));
        }
        dtp = mappedType->second;
      }

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
          st(std::exchange(col.sz, 0)) {}
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
  };

  using ColumnSet = typename std::vector<Column>;

  using MessageHandler = std::add_pointer<int(DBPROCESS *, DBINT, int, int, char *, char *, char *, int)>::type;
  using ErrorHandler = std::add_pointer<int(DBPROCESS *, int, int, int, char *, char *)>::type;

  class Connection {
   public:
    Connection() = delete;
    Connection(const DatabaseConfig &config, MessageHandler msgHandler = nullptr, ErrorHandler errHandler = nullptr)
        : dbproc(nullptr) {
      try {
        if (dbinit() == FAIL) {
          throw(std::runtime_error("dbinit() failed'"));
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
      dbexit();
    }

    RecordSet query(const std::string &queryString, const std::vector<int> &expectedTypes = {}) {
      return query(queryString.c_str(), expectedTypes);
    }

    RecordSet query(const char *queryString, const std::vector<int> &expectedTypes = {}) {
      try {
        if (dbproc == nullptr && DBISAVAIL(dbproc)) {
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

        ProcedureResult procResult = {getResultRows(expectedTypes), ReturnValueMap()};

        int numrets = dbnumrets(dbproc);

        for (auto i = 1; i <= numrets; i++) {
          auto retType = dbrettype(dbproc, i);
          std::string returnName(dbretname(dbproc, i));
          TypeValue it;

          switch (retType) {
            case SYBINT4: {
              it = *(reinterpret_cast<int *>(dbretdata(dbproc, i)));
              break;
            }

            case SYBVARCHAR: {
              it = std::string(reinterpret_cast<char *>(dbretdata(dbproc, i)), dbretlen(dbproc, i));
              break;
            }
          }

          procResult.returnValues[returnName] = std::move(it);
        }

        procResult.procedureReturnValue = std::nullopt;

        if (dbhasretstat(dbproc) == TRUE) {
          procResult.procedureReturnValue = dbretstatus(dbproc);
        }

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
                        it = *(reinterpret_cast<const int *>(buf));
                        break;
                      }

                      case TINYBIND: {
                        it = static_cast<uint8_t>(buf[0] && 0xFF);
                        break;
                      }

                      case SMALLBIND: {
                        it = *(reinterpret_cast<const uint16_t *>(buf));
                        break;
                      }

                      case REALBIND: {
                        it = *(reinterpret_cast<const float *>(buf));
                        break;
                      }

                      case FLT8BIND: {
                        it = *(reinterpret_cast<const double *>(buf));
                        break;
                      }

                      case NTBSTRINGBIND: {
                        it = std::string(buf);
                        break;
                      }

                      case DATETIMEBIND: {
                        it = *(reinterpret_cast<const DBDATETIME *>(buf));
                        break;
                      }
                    }
                  } else {
                    it = std::nullopt;
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
  };  // namespace MSSQLClient
}  // namespace MSSQLClient

#endif
