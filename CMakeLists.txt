cmake_minimum_required(VERSION 3.25)
project(smb C)

set(CMAKE_C_STANDARD 11)

add_executable(smbbroker smbbroker.c)
add_executable(smbpublish smbpublish.c)
add_executable(smbsubscribe smbsubscribe.c)
add_executable(smbcontipublish smbcontipublish.c)
