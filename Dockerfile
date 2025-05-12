# Use the official Python image as a base image
# FROM python:3.9-slim
FROM python:3.9

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Install Poetry
#RUN curl -sSL https://install.python-poetry.org | python3 - && \
#    # Add Poetry to PATH
#    export PATH="/root/.local/bin:$PATH"

# Set the working directory
WORKDIR /app

# 先复制依赖文件，利用 Docker 缓存
COPY poetry-1.7.1-py3-none-any.whl pyproject.toml poetry.lock ./

RUN pip3 config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple \ 
    && pip3 install poetry-1.7.1-py3-none-any.whl \
    && poetry lock --no-update && poetry install 

# Copy the rest of the application code into the image
COPY . .

RUN chmod +x ./init.sh 

ENV PYTHONPATH=/app

# Command to run the application
CMD ["bash", "./init.sh"]
