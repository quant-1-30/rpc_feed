# cython 重构 providers
# cython 重构 model and  util
# cython 重构 rpc server

    # register_date:登记日 ; ex_date:除权除息日 
    # 股权登记日后的下一个交易日就是除权日或除息日，这一天购入该公司股票的股东不再享有公司此次分红配股
    # 上交所证券的红股上市日为股权除权日的下一个交易日; 深交所证券的红股上市日为股权登记日后的第3个交易日
    # bonus_share --- 送股 / transfer --- 转股 / bonus --- 股息

    # register_date:登记日 ; ex_date:除权除息日; pay_date:除权除息日 ; effective_date:上市日期 
    # 股权登记日后的下一个交易日就是除权日或除息日，这一天购入该公司股票的股东不再享有公司此次分红配股
    # 上交所证券的红股上市日为股权除权日的下一个交易日; 深交所证券的红股上市日为股权登记日后的第3个交易日
    # price --- 配股价格 / ratio --- 配股比例

# blob to transform string to bytes
# alembic init alembic --template gener / async 
# alembic revision -m "change sid and name from str to bytes"

# python -m grpc_tools.protoc -I . --python_out=. --pyi_out=. --grpc_python_out=. service.proto 

# arrow string / bytes
<!-- buffers[0] → validity bitmap
buffers[1] → offsets (int32 / int64)
buffers[2] → data (byte blob) -->

# Arrow 格式**（如 DuckDB、Parquet 文件、Pandas DataFrame）时，转为 PyArrow 才有性能优势，因为那时是 **Zero-Copy**