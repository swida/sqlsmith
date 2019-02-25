/// @file
/// @brief schema and dut classes for SQLite 3

#ifndef SQLITE_HH
#define SQLITE_HH

extern "C"  {
#include <mysql.h>
}

#include "schema.hh"
#include "relmodel.hh"
#include "dut.hh"

struct mysql_connection {
  MYSQL *db;
  void q(const char *query);
  mysql_connection(std::string &conninfo);
  ~mysql_connection();
};

struct schema_mysql : schema, mysql_connection {
  schema_mysql(std::string &conninfo, bool no_catalog);
  virtual std::string quote_name(const std::string &id) {
    return id;
  }
  std::string quote(const std::string &id) {
    return "'" + id + "'";
  }
};

struct dut_mysql : dut_base, mysql_connection {
  virtual void test(const std::string &stmt);
  dut_mysql(std::string &conninfo);
};

#endif
