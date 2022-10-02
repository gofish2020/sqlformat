package sqlformat

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/gofish2020/gojson"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type pos struct {
	start int
	end   int
}

func replace(source []rune, start, end int, newstr string) []rune {
	result := []rune{}
	result = append(result, source[0:start]...)
	result = append(result, []rune(newstr)...)
	result = append(result, []rune(source[end+1:])...)
	return result
}

func formatsql(sql string, js *gojson.Json) (string, []interface{}) {
	sqlRune := []rune(strings.ToLower(sql)) //每个字符（兼容中文）
	args := make([]interface{}, 0)
	length := len(sqlRune)
	start, end := -1, -1
	posArray := make([]pos, 0)
	for i := 0; i < length; i++ {
		if sqlRune[i] == '#' && start == -1 && i+1 < length && sqlRune[i+1] == '{' {
			start = i
		}
		if start != -1 && sqlRune[i] == '}' {
			end = i //成对
			//截取字段名
			name := sqlRune[start+2 : end]

			if !js.Get(string(name)).Nil() {
				args = append(args, js.Get(string(name)).Interface())
				posArray = append(posArray, pos{start, end})
			}
			start, end = -1, -1
		}
	}
	//将占位符替换
	for i := len(posArray) - 1; i >= 0; i-- {
		sqlRune = replace(sqlRune, posArray[i].start, posArray[i].end, "?")
	}

	sql = string(sqlRune)
	return sql, args
}

/*
insert into db_user where (userid,nickname) values (#{userid},#{nickname})
*/
func ExecSql(cxt context.Context, db interface{}, sql string, js *gojson.Json) (sql.Result, error) {
	sql, args := formatsql(sql, js)
	//.Println("sql--->>>", sql)
	//执行sql语句
	switch x := db.(type) {
	case sqlx.SqlConn:
		return x.ExecCtx(cxt, sql, args...)
	case sqlx.Session:
		return x.ExecCtx(cxt, sql, args...)
	}
	return nil, errors.New("ExecSql db type is unknow")
}

/*
select * from db_user where userid=#{userid} and nickname=#{nickname}
*/
func QuerySql(ctx context.Context, db interface{}, sql string, js *gojson.Json, v interface{}) error {
	sql, args := formatsql(sql, js)
	//fmt.Println("sql--->>>", sql)
	//执行sql语句
	switch x := db.(type) {
	case sqlx.SqlConn:
		return x.QueryRowsCtx(ctx, v, sql, args...)
	case sqlx.Session:
		return x.QueryRowsCtx(ctx, v, sql, args...)
	}
	return errors.New("QuerySql db type is unknow")
}
