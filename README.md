

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

