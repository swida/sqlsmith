#include <stdexcept>
#include <cassert>
#include <cstring>
#include "mysql.hh"
#include <iostream>

#ifndef HAVE_BOOST_REGEX
#include <regex>
#else
#include <boost/regex.hpp>
using boost::regex;
using boost::smatch;
using boost::regex_match;
#endif

using namespace std;

static regex e_syntax(".*You have an error in your SQL syntax.*");
static regex e_user_abort("Query execution was interrupted");

extern "C"  {
#include <unistd.h>
}

mysql_connection::mysql_connection(std::string &conninfo)
{
  (void) conninfo;
  db = mysql_init(NULL);
  if (!mysql_real_connect(db, "127.0.0.1", "root", "", "test", 3306, "", 0))
    throw std::runtime_error(mysql_error(db));
}

void mysql_connection::q(const char *query)
{
  if (!mysql_real_query(db, query, strlen(query)))
    return;
  auto e = std::runtime_error(mysql_error(db));
  throw e;
}

mysql_connection::~mysql_connection()
{
  if (db)
    mysql_close(db);
}

schema_mysql::schema_mysql(std::string &conninfo, bool no_catalog)
  : mysql_connection(conninfo)
{
  std::string query = "SELECT table_schema, table_name FROM information_schema.tables WHERE engine = 'innodb'";
  if (no_catalog)
    query+=" AND table_schema NOT IN ('information_schema', 'sys', 'performance_schema', 'mysql')";

  cerr << "Loading tables...";

  int rc = mysql_real_query(db, query.c_str(), query.size());
  if (rc) {
    auto e = std::runtime_error(mysql_error(db));
    throw e;
  }

  MYSQL_ROW cur;
  MYSQL_RES *result = mysql_store_result(db);
  if (!result) {
    auto e = std::runtime_error(mysql_error(db));
    throw e;
  }

  while ((cur = mysql_fetch_row(result))) {
    table tab(cur[0], cur[1], true, true);
    tables.push_back(tab);
  }

  mysql_free_result(result);

  cerr << "done." << endl;

  cerr << "Loading columns and constraints...";

  for (auto t = tables.begin(); t != tables.end(); ++t) {
    string q("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = ");
    q += quote(t->schema);
    q += " AND table_name = ";
    q += quote(t->name);

    rc = mysql_real_query(db, q.c_str(), q.size());
    if (rc) {
      auto e = std::runtime_error(mysql_error(db));
      throw e;
    }

    if (!(result = mysql_store_result(db))) {
    auto e = std::runtime_error(mysql_error(db));
    throw e;
    }

    while ((cur = mysql_fetch_row(result))) {
      column c(cur[0], sqltype::get(cur[1]));
      t->columns().push_back(c);
    }

    mysql_free_result(result);
  }

  cerr << "done." << endl;

#define BINOP(n,t) do {op o(#n,sqltype::get(#t),sqltype::get(#t),sqltype::get(#t)); register_operator(o); } while(0)

  BINOP(||, TEXT);
  BINOP(*, INTEGER);
  BINOP(/, INTEGER);

  BINOP(+, INTEGER);
  BINOP(-, INTEGER);

  BINOP(>>, INTEGER);
  BINOP(<<, INTEGER);

  BINOP(&, INTEGER);
  BINOP(|, INTEGER);

  BINOP(<, INTEGER);
  BINOP(<=, INTEGER);
  BINOP(>, INTEGER);
  BINOP(>=, INTEGER);

  BINOP(=, INTEGER);
  BINOP(<>, INTEGER);
  BINOP(IS, INTEGER);
  BINOP(IS NOT, INTEGER);

  BINOP(AND, INTEGER);
  BINOP(OR, INTEGER);

#define FUNC(n,r) do {							\
    routine proc("", "", sqltype::get(#r), #n);				\
    register_routine(proc);						\
  } while(0)

#define FUNC1(n,r,a) do {						\
    routine proc("", "", sqltype::get(#r), #n);				\
    proc.argtypes.push_back(sqltype::get(#a));				\
    register_routine(proc);						\
  } while(0)

#define FUNC2(n,r,a,b) do {						\
    routine proc("", "", sqltype::get(#r), #n);				\
    proc.argtypes.push_back(sqltype::get(#a));				\
    proc.argtypes.push_back(sqltype::get(#b));				\
    register_routine(proc);						\
  } while(0)

#define FUNC3(n,r,a,b,c) do {						\
    routine proc("", "", sqltype::get(#r), #n);				\
    proc.argtypes.push_back(sqltype::get(#a));				\
    proc.argtypes.push_back(sqltype::get(#b));				\
    proc.argtypes.push_back(sqltype::get(#c));				\
    register_routine(proc);						\
  } while(0)

  FUNC(last_insert_rowid, INTEGER);
  FUNC(random, INTEGER);
  FUNC(sqlite_source_id, TEXT);
  FUNC(sqlite_version, TEXT);
  FUNC(total_changes, INTEGER);

  FUNC1(abs, INTEGER, REAL);
  FUNC1(hex, TEXT, TEXT);
  FUNC1(length, INTEGER, TEXT);
  FUNC1(lower, TEXT, TEXT);
  FUNC1(ltrim, TEXT, TEXT);
  FUNC1(quote, TEXT, TEXT);
  FUNC1(randomblob, TEXT, INTEGER);
  FUNC1(round, INTEGER, REAL);
  FUNC1(rtrim, TEXT, TEXT);
  FUNC1(soundex, TEXT, TEXT);
  FUNC1(sqlite_compileoption_get, TEXT, INTEGER);
  FUNC1(sqlite_compileoption_used, INTEGER, TEXT);
  FUNC1(trim, TEXT, TEXT);
  FUNC1(typeof, TEXT, INTEGER);
  FUNC1(typeof, TEXT, NUMERIC);
  FUNC1(typeof, TEXT, REAL);
  FUNC1(typeof, TEXT, TEXT);
  FUNC1(unicode, INTEGER, TEXT);
  FUNC1(upper, TEXT, TEXT);
  FUNC1(zeroblob, TEXT, INTEGER);

  FUNC2(glob, INTEGER, TEXT, TEXT);
  FUNC2(instr, INTEGER, TEXT, TEXT);
  FUNC2(like, INTEGER, TEXT, TEXT);
  FUNC2(ltrim, TEXT, TEXT, TEXT);
  FUNC2(rtrim, TEXT, TEXT, TEXT);
  FUNC2(trim, TEXT, TEXT, TEXT);
  FUNC2(round, INTEGER, REAL, INTEGER);
  FUNC2(substr, TEXT, TEXT, INTEGER);

  FUNC3(substr, TEXT, TEXT, INTEGER, INTEGER);
  FUNC3(replace, TEXT, TEXT, TEXT, TEXT);


#define AGG(n,r, a) do {						\
    routine proc("", "", sqltype::get(#r), #n);				\
    proc.argtypes.push_back(sqltype::get(#a));				\
    register_aggregate(proc);						\
  } while(0)

  AGG(avg, INTEGER, INTEGER);
  AGG(avg, REAL, REAL);
  AGG(count, INTEGER, REAL);
  AGG(count, INTEGER, TEXT);
  AGG(count, INTEGER, INTEGER);
  AGG(group_concat, TEXT, TEXT);
  AGG(max, REAL, REAL);
  AGG(max, INTEGER, INTEGER);
  AGG(sum, REAL, REAL);
  AGG(sum, INTEGER, INTEGER);
  AGG(total, REAL, INTEGER);
  AGG(total, REAL, REAL);

  booltype = sqltype::get("INTEGER");
  inttype = sqltype::get("INTEGER");

  internaltype = sqltype::get("internal");
  arraytype = sqltype::get("ARRAY");

  true_literal = "1";
  false_literal = "0";

  generate_indexes();
  mysql_close(db);
  db = NULL;
}

dut_mysql::dut_mysql(std::string &conninfo)
  : mysql_connection(conninfo)
{
  // q("PRAGMA main.auto_vacuum = 2");
}

void dut_mysql::test(const std::string &stmt)
{
  if (mysql_real_query(db, stmt.c_str(), stmt.size())) {
    const char *errmsg = mysql_error(db);
    try {
      if (regex_match(errmsg, e_syntax))
        throw dut::syntax(errmsg);
      else if (regex_match(errmsg, e_user_abort)) {
        return;
      } else
        throw dut::failure(errmsg);
    } catch (dut::failure &e) {
      throw;
    }
  }
}
