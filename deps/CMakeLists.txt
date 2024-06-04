# sql-parser
add_library(sql-parser STATIC
        "sql-parser/src/SQLParser.cpp"
        "sql-parser/src/SQLParserResult.cpp"
        "sql-parser/src/SQLParserResult.cpp"
        "sql-parser/src/parser/bison_parser.cpp"
        "sql-parser/src/parser/flex_lexer.cpp"
        "sql-parser/src/sql/Expr.cpp"
        "sql-parser/src/sql/PrepareStatement.cpp"
        "sql-parser/src/sql/SQLStatement.cpp"
        "sql-parser/src/sql/statements.cpp"
        "sql-parser/src/util/sqlhelper.cpp")

set_target_properties(sql-parser PROPERTIES POSITION_INDEPENDENT_CODE YES)

target_include_directories(sql-parser PRIVATE src)
target_include_directories(sql-parser PRIVATE src/sql)
target_include_directories(sql-parser PRIVATE src/parser)
target_include_directories(sql-parser PRIVATE src/util)

if (MSVC)
    target_compile_definitions(sql-parser PRIVATE YY_NO_UNISTD_H)
endif (MSVC)


# hiredis
add_library(hiredis STATIC
        "hiredis/alloc.c"
        "hiredis/async.c"
        "hiredis/dict.c"
        "hiredis/hiredis.c"
        "hiredis/net.c"
        "hiredis/read.c"
        "hiredis/sds.c"
        "hiredis/sockcompat.c")

set_target_properties(hiredis PROPERTIES POSITION_INDEPENDENT_CODE YES)
