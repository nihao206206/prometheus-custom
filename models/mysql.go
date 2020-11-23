package models

import (
	//"bytes"
	"database/sql"
	"container/list"
	// "encoding/json"
	// "fmt"
	"log"
	//"math"
	//"sort"
	//"strconv"
	"strings"
	//"time"

	blog "github.com/blog4go"
	_ "github.com/go-sql-driver/mysql"
	"github.com/orcaman/concurrent-map"
)

//规则
type RuleItem struct {
	Name         string
	Fn			 string
	Interval     int
    Alert        string
	Expr         string
	For			 string
	Labels		 map[string]string
	Annotations  map[string]string
	Metrics      string
}

type AlertDescSet struct {
	DimensMap cmap.ConcurrentMap
	TimesMap  cmap.ConcurrentMap
}

type AlertDescribe struct {
	Id        string
	DimenArry []string
}

func AlertDescSetConstructor() *AlertDescSet {
	return &AlertDescSet{DimensMap: cmap.New(), TimesMap: cmap.New()}
}

var (
	_SQL_DB 	*sql.DB
	_ERR    	error
	CND_LEFT    = "{"
	CND_RIGHT   = "}"
	SeriesAlertDesc = AlertDescSetConstructor()
)

func Initialization(db_url string) {
	_SQL_DB, _ERR = sql.Open("mysql", db_url)
	if _ERR != nil {
		log.Fatalf("Open database error: %s\n", _ERR)
		return
	}
	// defer _SQL_DB.Close()

	_ERR = _SQL_DB.Ping()
	if _ERR != nil {
		log.Fatal(_ERR)
		return
	}
}

func QueryRuleString() (*list.List, error) {
	
	var (
		mcname ,mname ,mcdime , rule_labels ,rule_annotations string
		mcs, mce    int
		dimenArray  []string
	)
	
	l := list.New()
	
	rows, err := _SQL_DB.Query("select rule_metrics,rule_name,rule_fn,rule_interval,rule_alert,rule_expr,rule_for,rule_labels,rule_annotations from rules;")
	if err != nil {
		log.Println(err)
	}
	defer rows.Close()

	for rows.Next() {
		var item RuleItem
		item.Labels=make(map[string]string)
		item.Annotations=make(map[string]string)
		err := rows.Scan(&mname,&item.Name,&item.Fn,&item.Interval,&item.Alert,&item.Expr,&item.For,&rule_labels,&rule_annotations)
		if err != nil {
			log.Fatal(err)
		}
		labels := strings.Split(rule_labels, ",")
		lablen := len(labels)
		for i:=0;i<lablen;i++ {
			blog.Debugf("### mysql.go  QueryRuleString i=%d ,labels=%s", i,labels[i])
			pars := strings.Split(labels[i], "=")
			plen := len(pars)
			for j:=0;j<plen;j+=2 {
				item.Labels[pars[j]]=pars[j+1]
			}
		}
		annotations := strings.Split(rule_annotations, ",")
		annlen := len(annotations)
		for k:=0;k<annlen;k++ {
			blog.Debugf("### mysql.go  QueryRuleString k=%d ,annotations=%s", k,annotations[k])
			pars := strings.Split(annotations[k], "=")
			plen := len(pars)
			for j:=0;j<plen;j+=2 {
				item.Annotations[pars[j]]=pars[j+1]
			}
		}
		
		mcs = strings.Index(mname, CND_LEFT)
		// 名称
		if mcs != -1 {
			mce = strings.Index(mname, CND_RIGHT)
			mcname = strings.TrimSpace(mname[0:mcs])
			// 维度
			mcdime = strings.TrimSpace(mname[mcs+1 : mce])
			dimenArray = strings.Split(mcdime, ",")
		} else {
			mcname = mname
			dimenArray = nil
		}
		
		if val, ok := SeriesAlertDesc.DimensMap.Get(mcname); ok {
			SeriesAlertDesc.DimensMap.Set(mcname, append(val.([]AlertDescribe), AlertDescribe{item.Alert, dimenArray}))
			blog.Debugf("### mysql.go  QueryRuleString mcname=%s , value = %v ,size=%d ", mcname, append(val.([]AlertDescribe), AlertDescribe{item.Alert, dimenArray}), SeriesAlertDesc.DimensMap.Count)
		} else {
			SeriesAlertDesc.DimensMap.Set(mcname, append([]AlertDescribe{}, AlertDescribe{item.Alert, dimenArray}))
			blog.Debugf("### mysql.go  QueryRuleString mcname=%s , value = %v ,size=%d ", mcname, AlertDescribe{item.Alert, dimenArray}, SeriesAlertDesc.DimensMap.Count)
		}
		
		l.PushBack(item)
		blog.Debugf("### mysql.go  QueryRuleString item=%s ,list len=%d ,labels=%s,annotations=%s", item,l.Len(),rule_labels,rule_annotations)
	}
	blog.Debugf("### mysql.go  QueryRuleString list=%s", l)

	return l, err
}
