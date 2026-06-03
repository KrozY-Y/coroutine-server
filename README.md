# Сборка bash:

g++-13 -std=c++20 -fcoroutines -o server server.cpp

#Запуск:

./server

#Подключение (Выведет "Hello, World!"): 

curl http://localhost:8080
