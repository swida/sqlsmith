#include <stdexcept>
#include <cassert>
#include <cstring>
#include "mysql.hh"
#include <iostream>

using namespace std;

extern "C"  {
#include <unistd.h>
#include <mysqld_error.h>

}

/*
 code borrowed from libpq
 */
static void conninfo_parse(const std::string conninfo,
                          std::string &host,
                           std::string &port,
                          std::string &user,
                          std::string &password,
                          std::string &database,
                          std::string &unix_socket)
{
  char	   *pname;
  char	   *pval;
  char	   *buf;
  char	   *cp;
  char	   *cp2;
  struct ci_option {
    const char *name;
    std::string &value;
  };
  ci_option options[] ={
    {"host", host}, {"port", port}, {"user", user},
     {"password", password}, {"database", database},
     {"unix_socket", unix_socket}};

  /* Need a modifiable copy of the input string */
  if ((buf = strdup(conninfo.c_str())) == NULL)
    throw std::runtime_error("out of memory");

  cp = buf;

  while (*cp)
  {
    /* Skip blanks before the parameter name */
    if (isspace((unsigned char) *cp))
    {
      cp++;
      continue;
    }

    /* Get the parameter name */
    pname = cp;
    while (*cp)
    {
      if (*cp == '=')
        break;
      if (isspace((unsigned char) *cp))
      {
        *cp++ = '\0';
        while (*cp)
        {
          if (!isspace((unsigned char) *cp))
            break;
          cp++;
        }
        break;
      }
      cp++;
    }

    /* Check that there is a following '=' */
    if (*cp != '=')
    {
      free(buf);
      throw std::runtime_error("missing \"=\" after \"%s\" in connection info string\n");
                             // pname);
    }
    *cp++ = '\0';

    /* Skip blanks after the '=' */
    while (*cp)
    {
      if (!isspace((unsigned char) *cp))
        break;
      cp++;
    }

    /* Get the parameter value */
    pval = cp;

    if (*cp != '\'')
    {
      cp2 = pval;
      while (*cp)
      {
        if (isspace((unsigned char) *cp))
        {
          *cp++ = '\0';
          break;
        }
        if (*cp == '\\')
        {
          cp++;
          if (*cp != '\0')
            *cp2++ = *cp++;
        }
        else
          *cp2++ = *cp++;
      }
      *cp2 = '\0';
    }
    else
    {
      cp2 = pval;
      cp++;
      for (;;)
      {
        if (*cp == '\0')
        {
          free(buf);
          throw std::runtime_error("unterminated quoted string in connection info string\n");
        }
        if (*cp == '\\')
        {
          cp++;
          if (*cp != '\0')
            *cp2++ = *cp++;
          continue;
        }
        if (*cp == '\'')
        {
          *cp2 = '\0';
          cp++;
          break;
        }
        *cp2++ = *cp++;
      }
    }

    /*
     * Now that we have the name and the value, store the record.
     */
    bool found = false;
    for(auto &option : options) {
      if (!strcmp(option.name, pname)) {
        found = true;
        option.value = pval;
      }
    }
    if (!found)
      throw std::runtime_error("no option found\n");
  }

  /* Done with the modifiable input string */
  free(buf);
}

mysql_connection::mysql_connection(std::string &conninfo)
{
  std::string host, port="3306", user,
    password, database, unix_socket;
  conninfo_parse(conninfo, host, port, user,
                 password, database, unix_socket);
  db = mysql_init(NULL);
  if (!mysql_real_connect(db, host.c_str(),
                          user.c_str(), password.c_str(), database.c_str()
                          , stol(port), "", 0))
    throw std::runtime_error(mysql_error(db));
}

void mysql_connection::q(const char *query)
{
  MYSQL_RES *result;
  if (mysql_real_query(db, query, strlen(query)))
    throw std::runtime_error(mysql_error(db));

  result = mysql_store_result(db);

  if (mysql_errno(db) != 0) {
      mysql_free_result(result);
    throw std::runtime_error(mysql_error(db));
    mysql_free_result(result);
  }
}

mysql_connection::~mysql_connection()
{
  if (db)
    mysql_close(db);
}

schema_mysql::schema_mysql(std::string &conninfo, bool no_catalog)
  : mysql_connection(conninfo)
{
  std::string query = "SELECT table_name, table_schema FROM information_schema.tables WHERE engine = 'innodb'";
  if (no_catalog)
    query+=" AND table_schema NOT IN ('information_schema', 'sys', 'performance_schema', 'mysql')";

  cerr << "Loading tables...";

  if (mysql_real_query(db, query.c_str(), query.size()))
    throw std::runtime_error(mysql_error(db));

  MYSQL_ROW cur;
  MYSQL_RES *result = mysql_store_result(db);
  if (mysql_errno(db) != 0) {
    mysql_free_result(result);
    throw std::runtime_error(mysql_error(db));
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
    if (mysql_real_query(db, q.c_str(), q.size()))
      throw std::runtime_error(mysql_error(db));

    result = mysql_store_result(db);
    if (mysql_errno(db) != 0) {
      mysql_free_result(result);
      throw std::runtime_error(mysql_error(db));
    }

    while ((cur = mysql_fetch_row(result))) {
      column c(cur[0], sqltype::get(cur[1]));
      t->columns().push_back(c);
    }

    mysql_free_result(result);
  }

  cerr << "done." << endl;

#define BINOP(n,t) do {op o(#n,sqltype::get(#t),sqltype::get(#t),sqltype::get(#t)); register_operator(o); } while(0)

  BINOP(||, INTEGER);
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

  FUNC(random, INTEGER);

  FUNC1(abs, INTEGER, REAL);
  FUNC1(hex, TEXT, TEXT);
  FUNC1(length, INTEGER, TEXT);
  FUNC1(lower, TEXT, TEXT);
  FUNC1(ltrim, TEXT, TEXT);
  FUNC1(quote, TEXT, TEXT);
  FUNC1(round, INTEGER, REAL);
  FUNC1(rtrim, TEXT, TEXT);
  FUNC1(trim, TEXT, TEXT);
  FUNC1(upper, TEXT, TEXT);

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

  booltype = sqltype::get("INTEGER");
  inttype = sqltype::get("INTEGER");

  internaltype = sqltype::get("internal");

  true_literal = "1";
  false_literal = "0";

  generate_indexes();
  mysql_close(db);
  db = NULL;
}

dut_mysql::dut_mysql(std::string &conninfo)
  : mysql_connection(conninfo)
{
  q("set force_parallel_mode=on");
  q("set max_parallel_degree=4");
  q("set max_execution_time = 3000");
}

void dut_mysql::test(const std::string &stmt)
{
  if (mysql_real_query(db, stmt.c_str(), stmt.size())) {
    unsigned int error = mysql_errno(db);
    const char *errmsg = mysql_error(db);

    try {
      if (error == ER_SYNTAX_ERROR)
        throw dut::syntax(errmsg);
      else if (error == ER_QUERY_TIMEOUT)
        throw dut::timeout(errmsg);
      else if (error == ER_QUERY_INTERRUPTED)
        return;
      else if (error == CR_SERVER_LOST) {
        mysql_close(db);
        db = NULL;
        throw dut::broken(errmsg);
      }
      else
        throw dut::failure(errmsg);
    } catch (dut::failure &e) {
      throw;
    }
  }

  MYSQL_RES *result = mysql_store_result(db);
  if (mysql_errno(db) != 0) {
    const char *errmsg = mysql_error(db);
    mysql_free_result(result);
    throw dut::failure(errmsg);
  }
  mysql_free_result(result);
}
