#include <stdio.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#define PORT 8081
#define IP "104.39.68.199"

int main(){
  int sock = 0, valread;
  struct sockaddr_in serv_addr;
  char* command1 = "connect";
  char* command2 = "write x 100 0 0";
  char* command3 = "write y 200 0 10";
  char* command4 = "read y";
  char* command5 = "write z 500 0 2";
  char* command6 = "show_dep";
  char* command7 = "disconnect";
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

  
  if(connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0){
    printf("Connect failed\n");
    return -1;
  }


  printf("Send connect request to server with ip: %s and port: %d\n", IP, PORT);
  send(sock, command1, strlen(command1), 0);
  valread = read(sock, buffer, 1024);
  printf("%s\n", buffer);

  /*
  printf("Send write request, to set value of x to be 100\n");
  memset(buffer, 0, sizeof(buffer));
  send(sock, command2, strlen(command2), 0);
  valread = read(sock, buffer, 1024);
  printf("%s\n", buffer);

  printf("Send write request, to set value of y to be 200\n"); 
  memset(buffer, 0, sizeof(buffer));
  send(sock, command3, strlen(command3), 0);
  valread = read(sock, buffer, 1024);
  printf("%s\n", buffer);
  */

  
  printf("Send read request, read value of y\n");
  memset(buffer, 0, sizeof(buffer));
  send(sock, command4, strlen(command4), 0);
  valread = read(sock, buffer, 1024);
  printf("%s\n", buffer);

  printf("Send write request, to set value of z to be 500\n");
  memset(buffer, 0, sizeof(buffer));
  send(sock, command5, strlen(command5), 0);
  valread = read(sock, buffer, 1024);
  printf("%s\n", buffer);
  


  printf("Send disconnect request\n");
  memset(buffer, 0, sizeof(buffer));
  send(sock, command7, strlen(command7), 0);
  valread = read(sock, buffer, 1024);
  printf("%s\n",buffer);



  return 0;
}
