%{

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "common/log/log.h"
#include "common/lang/string.h"
#include "sql/parser/parse_defs.h"
#include "sql/parser/yacc_sql.hpp"
#include "sql/parser/lex_sql.h"
#include "sql/expr/expression.h"
#include "sql/expr/subquery_expression.h"

using namespace std;

string token_name(const char *sql_string, YYLTYPE *llocp)
{
  return string(sql_string + llocp->first_column, llocp->last_column - llocp->first_column + 1);
}

int yyerror(YYLTYPE *llocp, const char *sql_string, ParsedSqlResult *sql_result, yyscan_t scanner, const char *msg)
{
  unique_ptr<ParsedSqlNode> error_sql_node = make_unique<ParsedSqlNode>(SCF_ERROR);
  error_sql_node->error.error_msg = msg;
  error_sql_node->error.line = llocp->first_line;
  error_sql_node->error.column = llocp->first_column;
  sql_result->add_sql_node(std::move(error_sql_node));
  return 0;
}

ArithmeticExpr *create_arithmetic_expression(ArithmeticExpr::Type type,
                                             Expression *left,
                                             Expression *right,
                                             const char *sql_string,
                                             YYLTYPE *llocp)
{
  ArithmeticExpr *expr = new ArithmeticExpr(type, left, right);
  expr->set_name(token_name(sql_string, llocp));
  return expr;
}

UnboundAggregateExpr *create_aggregate_expression(const char *aggregate_name,
                                           Expression *child,
                                           const char *sql_string,
                                           YYLTYPE *llocp)
{
  UnboundAggregateExpr *expr = new UnboundAggregateExpr(aggregate_name, child);
  expr->set_name(token_name(sql_string, llocp));
  return expr;
}

SubQueryExpr *create_subquery_expression(ParsedSqlNode* sql_node, const char *sql_string, YYLTYPE *llocp) {
 SubQueryExpr *expr = new SubQueryExpr(sql_node);
 expr->set_name(token_name(sql_string, llocp));
 return expr;
}

SubQueryExpr *create_subquery_expression(const vector<Value>& value_list) {
 SubQueryExpr *expr = new SubQueryExpr(value_list);
 return expr;
}

%}

%define api.pure full
%define parse.error verbose
/** 启用位置标识 **/
%locations
%lex-param { yyscan_t scanner }
/** 这些定义了在yyparse函数中的参数 **/
%parse-param { const char * sql_string }
%parse-param { ParsedSqlResult * sql_result }
%parse-param { void * scanner }

//标识tokens
%token  SEMICOLON
        BY
        CREATE
        DROP
        GROUP
        VIEW
        TABLE
        TABLES
        INDEX
        CALC
        SELECT
        DESC
        ASC
        SHOW
        SYNC
        INSERT
        DELETE
        UPDATE
        LBRACE
        RBRACE
        COMMA
        TRX_BEGIN
        TRX_COMMIT
        TRX_ROLLBACK
        INT_T
        STRING_T
        FLOAT_T
        VECTOR_T
        DATE_T
        HELP
        EXIT
        DOT //QUOTE
        INTO
        VALUES
        FROM
        WHERE
        AND
        OR
        SET
        ON
        LOAD
        EXPLAIN
        STORAGE
        FORMAT
        PRIMARY
        KEY
        ANALYZE
        NOT
        NULL_T
        IS_T
        INNER_T
        JOIN_T
        IN_T
        ORDER_T
        TEXT_T
        LIKE_T
        AS_T
        HAVING_T
        UNIQUE_T
        EQ
        LT
        GT
        LE
        GE
        NE

/** union 中定义各种数据类型，真实生成的代码也是union类型，所以不能有非POD类型的数据 **/
%union {
  ParsedSqlNode *                            sql_node;
  Value *                                    value;
  vector<Value>*                             value_list;
  enum CompOp                                comp;
  RelAttrSqlNode *                           rel_attr;
  vector<AttrInfoSqlNode> *                  attr_infos;
  AttrInfoSqlNode *                          attr_info;
  Expression *                               expression;
  vector<unique_ptr<Expression>> *           expression_list;
  vector<RelAttrSqlNode> *                   rel_attr_list;
  UnboundTable*                              table_ref;
  vector<string> *                           key_list;
  Assignment *                               assignment;
  std::vector<unique_ptr<Assignment>> *      assignment_list;
  OrderBy*                                   order_by_entry;
  vector<unique_ptr<OrderBy>>*               order_by_list;
  GroupBy*                                   group_by;
  char *                                     cstring;
  int                                        number;
  float                                      floats;
  bool                                       boolean;
}

%token <number> NUMBER
%token <floats> FLOAT
%token <cstring> ID
%token <cstring> SSS
//非终结符

/** type 定义了各种解析后的结果输出的是什么类型。类型对应了 union 中的定义的成员变量名称 **/
%type <boolean>             null_opt;
%type <number>              type
%type <value>               value
%type <value_list>          value_list
%type <number>              number
%type <cstring>             relation
%type <comp>                comp_op
%type <comp>                in_opt
%type <comp>                like_opt
%type <cstring>             alias_opt
%type <boolean>             unique_opt 
%type <rel_attr>            rel_attr
%type <attr_infos>          attr_def_list
%type <attr_info>           attr_def
%type <expression>          where
%type <cstring>             storage_format
%type <key_list>            primary_key
%type <key_list>            attr_list
%type <table_ref>           table_refs
%type <table_ref>           table_factor
%type <table_ref>           joined_table
%type <expression>          expression
%type <expression>          boolean_primary
%type <expression>          predicate
%type <expression>          bit_expr
%type <expression>          simple_expr
%type <expression>          subquery_expr 
%type <expression_list>     expression_list
%type <group_by>            group_by
%type <order_by_list>       order_by 
%type <assignment>          assignment
%type <assignment_list>     assignment_list
%type <order_by_list>       order_by_list
%type <order_by_entry>      order_by_entry
%type <sql_node>            calc_stmt
%type <sql_node>            select_stmt
%type <sql_node>            select_stmt_opt
%type <sql_node>            insert_stmt
%type <sql_node>            update_stmt
%type <sql_node>            delete_stmt
%type <sql_node>            create_view_stmt
%type <sql_node>            create_table_stmt
%type <sql_node>            drop_table_stmt
%type <sql_node>            analyze_table_stmt
%type <sql_node>            show_tables_stmt
%type <sql_node>            desc_table_stmt
%type <sql_node>            create_index_stmt
%type <sql_node>            drop_index_stmt
%type <sql_node>            sync_stmt
%type <sql_node>            begin_stmt
%type <sql_node>            commit_stmt
%type <sql_node>            rollback_stmt
%type <sql_node>            explain_stmt
%type <sql_node>            set_variable_stmt
%type <sql_node>            help_stmt
%type <sql_node>            exit_stmt
%type <sql_node>            command_wrapper
// commands should be a list but I use a single command instead
%type <sql_node>            commands

%left '+' '-'
%left '*' '/'
%left AND OR
%right UMINUS
%%

commands: command_wrapper opt_semicolon  //commands or sqls. parser starts here.
  {
    unique_ptr<ParsedSqlNode> sql_node = unique_ptr<ParsedSqlNode>($1);
    sql_result->add_sql_node(std::move(sql_node));
  }
  ;

command_wrapper:
    calc_stmt
  | select_stmt
  | insert_stmt
  | update_stmt
  | delete_stmt
  | create_view_stmt
  | create_table_stmt
  | drop_table_stmt
  | analyze_table_stmt
  | show_tables_stmt
  | desc_table_stmt
  | create_index_stmt
  | drop_index_stmt
  | sync_stmt
  | begin_stmt
  | commit_stmt
  | rollback_stmt
  | explain_stmt
  | set_variable_stmt
  | help_stmt
  | exit_stmt
    ;

exit_stmt:      
    EXIT {
      (void)yynerrs;  // 这么写为了消除yynerrs未使用的告警。如果你有更好的方法欢迎提PR
      $$ = new ParsedSqlNode(SCF_EXIT);
    };

help_stmt:
    HELP {
      $$ = new ParsedSqlNode(SCF_HELP);
    };

sync_stmt:
    SYNC {
      $$ = new ParsedSqlNode(SCF_SYNC);
    }
    ;

begin_stmt:
    TRX_BEGIN  {
      $$ = new ParsedSqlNode(SCF_BEGIN);
    }
    ;

commit_stmt:
    TRX_COMMIT {
      $$ = new ParsedSqlNode(SCF_COMMIT);
    }
    ;

rollback_stmt:
    TRX_ROLLBACK  {
      $$ = new ParsedSqlNode(SCF_ROLLBACK);
    }
    ;

drop_table_stmt:    /*drop table 语句的语法解析树*/
    DROP TABLE ID {
      $$ = new ParsedSqlNode(SCF_DROP_TABLE);
      $$->drop_table.relation_name = $3;
    };

analyze_table_stmt:  /* analyze table 语法的语法解析树*/
    ANALYZE TABLE ID {
      $$ = new ParsedSqlNode(SCF_ANALYZE_TABLE);
      $$->analyze_table.relation_name = $3;
    }
    ;

show_tables_stmt:
    SHOW TABLES {
      $$ = new ParsedSqlNode(SCF_SHOW_TABLES);
    }
    ;

desc_table_stmt:
    DESC ID  {
      $$ = new ParsedSqlNode(SCF_DESC_TABLE);
      $$->desc_table.relation_name = $2;
    }
    ;

create_index_stmt:    /*create index 语句的语法解析树*/
    CREATE unique_opt INDEX ID ON ID LBRACE attr_list RBRACE
    {
      $$ = new ParsedSqlNode(SCF_CREATE_INDEX);
      CreateIndexSqlNode &create_index = $$->create_index;
      create_index.is_unique = $2;
      create_index.index_name = $4;
      create_index.relation_name = $6;
      // create_index.attribute_name = $7;
      create_index.attr_names.swap(*$8);
      delete $8;
    }
    ;

drop_index_stmt:      /*drop index 语句的语法解析树*/
    DROP INDEX ID ON ID
    {
      $$ = new ParsedSqlNode(SCF_DROP_INDEX);
      $$->drop_index.index_name = $3;
      $$->drop_index.relation_name = $5;
    }
    ;
create_view_stmt:
    CREATE VIEW ID AS_T select_stmt 
    {
      $$ = new ParsedSqlNode(SCF_CREATE_VIEW);
      CreateViewSqlNode &create_view = $$->create_view;
      create_view.view_name = $3;
      create_view.select_sql = token_name(sql_string, &@5);
      delete $5;
    }
    | CREATE VIEW ID LBRACE attr_list RBRACE AS_T select_stmt 
    {
      $$ = new ParsedSqlNode(SCF_CREATE_VIEW);
      CreateViewSqlNode &create_view = $$->create_view;
      create_view.view_name = $3;
      create_view.attr_names.swap(*$5);
      delete $5;
      create_view.select_sql = token_name(sql_string, &@8);
      delete $8;
    }
    ;
create_table_stmt:    /*create table 语句的语法解析树*/
    CREATE TABLE ID LBRACE attr_def attr_def_list primary_key RBRACE select_stmt_opt storage_format
    {
      $$ = new ParsedSqlNode(SCF_CREATE_TABLE);
      CreateTableSqlNode &create_table = $$->create_table;
      create_table.relation_name = $3;

      vector<AttrInfoSqlNode> *src_attrs = $6;

      if (src_attrs != nullptr) {
        create_table.attr_infos.swap(*src_attrs);
        delete src_attrs;
      }
      create_table.attr_infos.emplace_back(*$5);
      reverse(create_table.attr_infos.begin(), create_table.attr_infos.end());
      delete $5;
      if ($7 != nullptr) {
        create_table.primary_keys.swap(*$7);
        delete $7;
      }
      if ($9 != nullptr) {
        create_table.select_sql_node = &($9->selection);
      }
      else {
        create_table.select_sql_node = nullptr;
      }
      if ($10 != nullptr) {
        create_table.storage_format = $10;
      }
    }
    | CREATE TABLE ID select_stmt_opt storage_format {
      $$ = new ParsedSqlNode(SCF_CREATE_TABLE);
      CreateTableSqlNode &create_table = $$->create_table;
      create_table.relation_name = $3;
      if ($4 != nullptr) {
        create_table.select_sql_node = &($4->selection);
      }
      else {
        create_table.select_sql_node = nullptr;
      }
      if ($5 != nullptr) {
        create_table.storage_format = $5;
      }
    }
    ;
    
attr_def_list:
    /* empty */
    {
      $$ = nullptr;
    }
    | attr_def_list COMMA attr_def
    {
      if ($1 != nullptr) {
        $$ = $1;
      } else {
        $$ = new vector<AttrInfoSqlNode>;
      }
      $$->insert($$->begin(), *$3);
      delete $3;
    }
    ;

null_opt:    
    /* empty */
    {
      $$ = true;
    }
    | NULL_T
    {
      $$ = true;
    }
    | NOT NULL_T
    {
      $$ = false;
    }
    ;

attr_def:
    ID type LBRACE number RBRACE null_opt
    {
      $$ = new AttrInfoSqlNode;
      $$->type = (AttrType)$2;
      $$->name = $1;
      if ($$->type == AttrType::VECTORS) {
        $$->length = $4 * sizeof(float);
      }
      else {
        $$->length = $4;
      }
      $$->nullable = $6;
    }
    | ID type null_opt
    {
      $$ = new AttrInfoSqlNode;
      $$->type = (AttrType)$2;
      $$->name = $1;
      $$->length = 4;
      $$->nullable = $3;
    }
    ;
number:
    NUMBER {$$ = $1;}
    ;
type:
    INT_T      { $$ = static_cast<int>(AttrType::INTS); }
    | STRING_T { $$ = static_cast<int>(AttrType::CHARS); }
    | FLOAT_T  { $$ = static_cast<int>(AttrType::FLOATS); }
    | VECTOR_T { $$ = static_cast<int>(AttrType::VECTORS); }
    | DATE_T   { $$ = static_cast<int>(AttrType::DATES); }
    | TEXT_T   { $$ = static_cast<int>(AttrType::TEXT); }
    ;
primary_key:
    /* empty */
    {
      $$ = nullptr;
    }
    | COMMA PRIMARY KEY LBRACE attr_list RBRACE
    {
      $$ = $5;
    }
    ;

attr_list:
    ID {
      $$ = new vector<string>();
      $$->push_back($1);
    }
    | ID COMMA attr_list {
      if ($3 != nullptr) {
        $$ = $3;
      } else {
        $$ = new vector<string>;
      }

      $$->insert($$->begin(), $1);
    }
    ;

insert_stmt:        /*insert   语句的语法解析树*/
    INSERT INTO ID VALUES LBRACE expression_list RBRACE 
    {
      $$ = new ParsedSqlNode(SCF_INSERT);
      $$->insertion.relation_name = $3;
      if ($6 != nullptr) {
        for (const std::unique_ptr<Expression>& expr: *$6) {
          Value val;
          if (OB_FAIL(expr->try_get_value(val))) {
            delete $6;
            YYERROR;
          }
          $$->insertion.values.push_back(val);
        }
        delete $6;
      }
    }
    ;

value:
    NUMBER {
      $$ = new Value((int)$1);
      @$ = @1;
    }
    |FLOAT {
      $$ = new Value((float)$1);
      @$ = @1;
    }
    |SSS {
      char *tmp = common::substr($1,1,strlen($1)-2);
      $$ = new Value(tmp);
      free(tmp);
    }
    |NULL_T {
      $$ = new Value((int)0);
      $$->set_null(true);
      @$ = @1;
    }
    ;

value_list:
    expression 
    {
      $$ = new vector<Value>;
      Value val;
      if (OB_FAIL($1->try_get_value(val))) {
        YYERROR;
      }
      $$->emplace_back(val);
      delete $1;
    }
    | expression COMMA value_list 
    {
      if ($3 != nullptr) {
        $$ = $3;
      } else {
        $$ = new vector<Value>;
      }
      Value val;
      if (OB_FAIL($1->try_get_value(val))) {
        YYERROR;
      }
      $$->emplace($$->begin(), val);
      delete $1;
    }
    ;

storage_format:
    /* empty */
    {
      $$ = nullptr;
    }
    | STORAGE FORMAT EQ ID
    {
      $$ = $4;
    }
    ;
    
delete_stmt:    /*  delete 语句的语法解析树*/
    DELETE FROM ID where 
    {
      $$ = new ParsedSqlNode(SCF_DELETE);
      $$->deletion.relation_name = $3;
      if ($4 != nullptr) {
        $$->deletion.condition.reset($4);
      }
    }
    ;

assignment:
    ID EQ expression
    {
      $$ = new Assignment();
      $$->attribute_name = $1;
      $$->expr = $3;
    }
    ;
assignment_list:
    assignment 
    {
      $$ = new vector<unique_ptr<Assignment>>;
      $$->emplace_back($1);
    }
    | assignment COMMA assignment_list
    {
      if ($3 != nullptr) {
        $$ = $3;
      } else {
        $$ = new vector<unique_ptr<Assignment>>;
      }
      $$->emplace($$->begin(), $1);
    }
    ;


update_stmt:      /*  update 语句的语法解析树*/
    UPDATE relation SET assignment_list where 
    {
      $$ = new ParsedSqlNode(SCF_UPDATE);
      $$->update.relation_name = $2;
      if ($4 != nullptr) {
        $$->update.assignment_list.swap(*$4);
        delete $4;
      }
      if ($5 != nullptr) {
        $$->update.condition.reset($5);
      }
    }
    ;
select_stmt:        /*  select 语句的语法解析树*/
    SELECT expression_list FROM table_refs where group_by order_by
    {
      $$ = new ParsedSqlNode(SCF_SELECT);
      if ($2 != nullptr) {
        $$->selection.expressions.swap(*$2);
        delete $2;
      }

      if ($4 != nullptr) {
        // $$->selection.relations.swap(*$4);
        $$->selection.table_refs.reset($4);
      }

      if ($5 != nullptr) {
        $$->selection.condition.reset($5);
      }

      if ($6 != nullptr) {
        // $$->selection.group_by.swap(*$6);
        $$->selection.group_by.reset($6);
      }

      if ($7 != nullptr) {
        $$->selection.order_by.swap(*$7);
        delete $7;
      }
    }
    ;

select_stmt_opt:
    {
      $$ = nullptr;
    }
    | select_stmt {
      $$ = $1;
    }
    | AS_T select_stmt {
      $$ = $2;
    }
    ;

calc_stmt:
    CALC expression_list
    {
      $$ = new ParsedSqlNode(SCF_CALC);
      $$->calc.expressions.swap(*$2);
      delete $2;
    }
    ;

expression_list:
    expression alias_opt
    {
      if ($1->type() == ExprType::STAR && $2 != nullptr) {
        YYERROR;
      }
      if ($2 != nullptr) {
        $1->set_alias_name($2);
      }
      $$ = new vector<unique_ptr<Expression>>;
      $$->emplace_back($1);
    }
    | expression alias_opt COMMA expression_list
    {
      if ($4 != nullptr) {
        $$ = $4;
      } else {
        $$ = new vector<unique_ptr<Expression>>;
      }
      if ($2 != nullptr) {
        $1->set_alias_name($2);
      }
      $$->emplace($$->begin(), $1);
    }
    ;

expression:
    boolean_primary {
      $$ = $1;
    }
    | expression AND expression {
      $$ = new ConjunctionExpr(ConjunctionExpr::Type::AND, $1, $3);
      $$->set_name(token_name(sql_string, &@$));
    }
    | expression OR expression {
      $$ = new ConjunctionExpr(ConjunctionExpr::Type::OR, $1, $3);
      $$->set_name(token_name(sql_string, &@$));
    }
    ;

boolean_primary:
    boolean_primary comp_op predicate {
      $$ = new ComparisonExpr($2, $1, $3);
      $$->set_name(token_name(sql_string, &@$));
    }
    | predicate {
      $$ = $1;
    }
    ;

predicate:
    bit_expr {
      $$ = $1;
    }
    | bit_expr like_opt simple_expr {
      $$ = new ComparisonExpr($2, $1, $3);
      $$->set_name(token_name(sql_string, &@$));
    }
    | bit_expr in_opt LBRACE value_list RBRACE {
      SubQueryExpr* subquery_expr = create_subquery_expression(*$4);
      $$ = new ComparisonExpr($2, $1, subquery_expr);
      $$->set_name(token_name(sql_string, &@$));
      delete $4;
    }
    | bit_expr in_opt subquery_expr {
      $$ = new ComparisonExpr($2, $1, $3);
      $$->set_name(token_name(sql_string, &@$));
    }
    ;


bit_expr:
    bit_expr '+' bit_expr {
      $$ = create_arithmetic_expression(ArithmeticExpr::Type::ADD, $1, $3, sql_string, &@$);
    }
    | bit_expr '-' bit_expr {
      $$ = create_arithmetic_expression(ArithmeticExpr::Type::SUB, $1, $3, sql_string, &@$);
    }
    | bit_expr '*' bit_expr {
      $$ = create_arithmetic_expression(ArithmeticExpr::Type::MUL, $1, $3, sql_string, &@$);
    }
    | bit_expr '/' bit_expr {
      $$ = create_arithmetic_expression(ArithmeticExpr::Type::DIV, $1, $3, sql_string, &@$);
    }
    | simple_expr {
      $$ = $1;
    }
    ;

simple_expr:
    value {
      $$ = new ValueExpr(*$1);
      $$->set_name(token_name(sql_string, &@$));
      delete $1;
    }
    | rel_attr {
      RelAttrSqlNode *node = $1;
      $$ = new UnboundFieldExpr(node->relation_name, node->attribute_name);
      $$->set_name(token_name(sql_string, &@$));
      delete $1;
    }
    | '*' {
      $$ = new StarExpr();
    }
    | ID DOT '*' {
      $$ = new StarExpr($1);
    }
    | LBRACE expression RBRACE {
      $$ = $2;
      $$->set_name(token_name(sql_string, &@$));
    }
    | '-' simple_expr %prec UMINUS {
      $$ = create_arithmetic_expression(ArithmeticExpr::Type::NEGATIVE, $2, nullptr, sql_string, &@$);
    }
    | ID LBRACE expression RBRACE {
      $$ = create_aggregate_expression($1, $3, sql_string, &@$);
    }
    | subquery_expr {
      $$ = $1;
    }
    ;

subquery_expr:
    LBRACE select_stmt RBRACE {
      $$ = create_subquery_expression($2, sql_string, &@$);
    }
    ;

rel_attr:
    ID {
      $$ = new RelAttrSqlNode;
      $$->attribute_name = $1;
    }
    | ID DOT ID {
      $$ = new RelAttrSqlNode;
      $$->relation_name  = $1;
      $$->attribute_name = $3;
    }
    ;

relation:
    ID {
      $$ = $1;
    }
    ;

table_refs:
    table_factor {
      $$ = $1;
    }
    | joined_table {
      $$ = $1;
    }
    ;

table_factor:
    relation alias_opt {
      UnboundSingleTable* tmp = new UnboundSingleTable();
      tmp->relation_name = $1;
      if ($2 != nullptr) {
        tmp->alias_name = $2;
      }
      $$ = tmp;
    }
    ;

joined_table:
    table_refs COMMA table_factor {
      UnboundJoinedTable* tmp = new UnboundJoinedTable();
      tmp->type = JoinType::CROSS;
      tmp->left.reset($1);
      tmp->right.reset($3);
      $$ = tmp;
    }
    | table_refs INNER_T JOIN_T table_factor ON expression {
      UnboundJoinedTable* tmp = new UnboundJoinedTable();
      tmp->type = JoinType::INNER;
      tmp->left.reset($1);
      tmp->right.reset($4);
      tmp->expr.reset($6);
      $$ = tmp;
    }
    ;

where:
    /* empty */
    {
      $$ = nullptr;
    }
    | WHERE expression {
      $$ = $2;  
    }
    ;

comp_op:
      EQ { $$ = EQUAL_TO; }
    | LT { $$ = LESS_THAN; }
    | GT { $$ = GREAT_THAN; }
    | LE { $$ = LESS_EQUAL; }
    | GE { $$ = GREAT_EQUAL; }
    | NE { $$ = NOT_EQUAL; }
    | IS_T { $$ = IS; }
    | IS_T NOT { $$ = IS_NOT; }
    // | IN_T { $$ = IN; }
    // | NOT IN_T { $$ = NOT_IN; }
    ;

in_opt:
    IN_T { $$ = IN; }
    | NOT IN_T { $$ = NOT_IN; }
    ;

like_opt:
    LIKE_T { $$ = LIKE; }
    | NOT LIKE_T { $$ = NOT_LIKE; }
    ;

alias_opt:
    {
      $$ = nullptr;
    }
    | ID {
      $$ = $1;
    }
    | AS_T ID {
      $$ = $2;
    }
    ;
unique_opt:
    {
      $$ = false;
    }
    | UNIQUE_T {
      $$ = true;
    }
    ;
group_by:
    /* empty */
    {
      $$ = nullptr;
    }
    | GROUP BY expression_list {
      $$ = new GroupBy;
      $$->exprs.swap(*$3);
      delete $3;
      $$->having_predicate = nullptr;
    }
    | GROUP BY expression_list HAVING_T expression {
      $$ = new GroupBy;
      $$->exprs.swap(*$3);
      delete $3;
      $$->having_predicate.reset($5);
    }
    ;
order_by_entry:
    expression {
      $$ = new OrderBy;
      $$->is_asc = true;
      $$->expr.reset($1);
    }
    | expression ASC {
      $$ = new OrderBy;
      $$->is_asc = true;
      $$->expr.reset($1);
    }
    | expression DESC {
      $$ = new OrderBy;
      $$->is_asc = false;
      $$->expr.reset($1);
    }
    ;
order_by_list:
    order_by_entry {
      $$ = new vector<unique_ptr<OrderBy>>;
      $$->emplace_back($1);
    }
    | order_by_entry COMMA order_by_list {
      if ($3 != nullptr) {
        $$ = $3;
      }
      else {
        $$ = new vector<unique_ptr<OrderBy>>;
      }
      $$->emplace($$->begin(), $1);
    }
    ;
order_by:
    {
      $$ = nullptr;
    }
    | ORDER_T BY order_by_list {
      $$ = $3;
    }
    ;

explain_stmt:
    EXPLAIN command_wrapper
    {
      $$ = new ParsedSqlNode(SCF_EXPLAIN);
      $$->explain.sql_node = unique_ptr<ParsedSqlNode>($2);
    }
    ;

set_variable_stmt:
    SET ID EQ value
    {
      $$ = new ParsedSqlNode(SCF_SET_VARIABLE);
      $$->set_variable.name  = $2;
      $$->set_variable.value = *$4;
      delete $4;
    }
    ;

opt_semicolon: /*empty*/
    | SEMICOLON
    ;
%%
//_____________________________________________________________________
extern void scan_string(const char *str, yyscan_t scanner);

int sql_parse(const char *s, ParsedSqlResult *sql_result) {
  yyscan_t scanner;
  std::vector<char *> allocated_strings;
  yylex_init_extra(static_cast<void*>(&allocated_strings),&scanner);
  scan_string(s, scanner);
  int result = yyparse(s, sql_result, scanner);

  for (char *ptr : allocated_strings) {
    free(ptr);
  }
  allocated_strings.clear();

  yylex_destroy(scanner);
  return result;
}
