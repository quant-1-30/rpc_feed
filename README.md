framework:

serialize: pb / arrow / fury

# pb 
python -m grpc_tools.protoc -I . --python_out=. --pyi_out=. --grpc_python_out=. service.proto 

# fb
flatc --grpc --python -o . -I . service.fbs


# 一元模式(在一次调用中, 客户端只能向服务器传输一次请求数据, 服务器也只能返回一次响应)
# 客户端流模式（在一次调用中, 客户端可以多次向服务器传输数据, 但是服务器只能返回一次响应）
# 服务端流模式（在一次调用中, 客户端只能一次向服务器传输数据, 但是服务器可以多次返回响应）
# 双向流模式 (在一次调用中, 客户端和服务器都可以向对方多次收发数据)

# peewee / sqlalchemy

mysql tool:
    # select host,user,authentication_string from mysql.user;
    # set password for user@localhost = newpassword;
    # flush privileges;
    # create user c_test@locahost identified by password;
    # drop user c_test@localhost;
    # grant select,update | all privileges on orm.* to guest@localhost;
    # 当忘记密码:  mysqld --skip-grant-tables,use mysql,set password
    # primary key
    # constraint
    # foreign key references
    # alter add constraint
    # alter  drop foreign key

# frame.sort_values(by=self.p.sort_key, ascending=True, inplace=True)
# multi_index = pd.MultiIndex.from_arrays(_sids, names=self.p.lines)

# 优化存在 ---- 1、 分库、分表 ; 2、异步入库; 3、多线程处理 30亿条的数据, 每天增加规模115万条
单表数据控制1亿条内 ---> 按照季度划分

1\ 分表 
2\ 执行 struct data
3\ df -h --- mac外接硬盘
4\ rpc_server --- original data / quote --- calc adjustment coef as line pass to sdk
5\  data sync  trading_day / asset 

ALTER TABLE table_name 
ALTER COLUMN column_name TYPE new_data_type;
