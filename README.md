数据        来源
1minute    通达信
分红        交易所数据
退市        交易所数据
基本信息     交易所数据
交易制度     人工整理（竞价机制 / 限制 ）

交易所获取包含股票基本信息，包含退市信息
需要从新浪爬取数据爬深证交易所股票分红与分股数据

工具:
通达信工具 解析struct数据
scrapy 定时爬群交易所官网数据

架构

     scrapy       middleware     
                                                                              ws / rpc / http
          download  --------------------- parser  ------------------ dump -------------------- server


serialize: pb / arrow / fury

# pb 
python -m grpc_tools.protoc -I . --python_out=. --pyi_out=. --grpc_python_out=. service.proto 

# fb
flatc --grpc --python -o . -I . service.fbs

# 优化存在 ---- 1、 分库、分表 ; 2、异步入库; 3、多线程处理 30亿条的数据, 每天增加规模115万条

# 转债价格超过130的，大部分都满足强赎条件了， 有的可转债可以没回售条款
# put_price 可转债发行价格
# convert_price 转股价格
# put_convert_price 回售触发价 --- 触发条款为连续30个交易日为convert_price的70%        
# force_redeem_price 强制赎回价 --- 强制赎回价条款为连续30个交易日中不少于15个交易日为convert_price的130%
# redeem_price 触发了强制赎回条款之后赎回未转股的可转债的价格