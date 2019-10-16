#include <stdio.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#define PORT 8080
#define IP "192.168.1.8"

int main(){
  int sock = 0, valread;
  struct sockaddr_in serv_addr;
  char* command1 = "connect";
  char* command2 = "write y 2";
  char* command6 = "write y 1";
  char* command7 = "write x 3";
  char* command3 = "read x";
  char* command4 = "read y";
  char* command5 = "show_dep";
  char* command = "disconnect";
  char buffer[1024] = {0};
  if((sock = socket(AF_INET, SOCK_STREAM, 0)) <0){
    printf("\n socket creation error\n");
    return -1;
  }

  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(PORT);
  
  if(inet_pton(AF_INET, IP, &serv_addr.sin_addr)<=0){
    printf("invalide address\n");
    return -1;
  }

  printf("try connecting\n");
  
  if(connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0){
    printf("Connect failed\n");
    return -1;
  }

  send(sock, command1, strlen(command1), 0);
  valread = read(sock, buffer, 1024);
  printf("%s\n", buffer);

  memset(buffer, 0, sizeof(buffer));
  send(sock, command2, strlen(command2), 0);
  valread = read(sock, buffer, 1024);
  printf("%s\n", buffer);

  memset(buffer, 0, sizeof(buffer));
  send(sock, command6, strlen(command6), 0);
  valread = read(sock, buffer, 1024);
  printf("%s\n", buffer);

  memset(buffer, 0, sizeof(buffer));
  send(sock, command7, strlen(command7), 0);
  valread = read(sock, buffer, 1024);
  printf("%s\n", buffer);

  memset(buffer, 0, sizeof(buffer));
  send(sock, command3, strlen(command3), 0);
  valread = read(sock, buffer, 1024);
  printf("%s\n", buffer);

  memset(buffer, 0, sizeof(buffer));
  send(sock, command4, strlen(command4), 0);
  valread = read(sock, buffer, 1024);
  printf("%s\n", buffer);

  memset(buffer, 0, sizeof(buffer));
  send(sock, command5, strlen(command5), 0);
  valread = read(sock, buffer, 1024);
  printf("%s\n",buffer);

  
  memset(buffer, 0, sizeof(buffer));
  send(sock, command, strlen(command), 0);
  valread = read(sock, buffer, 1024);
  printf("%s\n",buffer);


  printf("Hello message sent\n");

  printf("%s\n", buffer);
  return 0;
}
