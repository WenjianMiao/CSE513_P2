#include <unistd.h>
#include <stdio.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>
#include <map>
#include <string>
#include <iostream>
#include <vector>
#include <tuple>
#include <errno.h>
#include <pthread.h>
#define PORT 8080

using namespace std;

int GlobalTime = 0;

int my_id = 0;

void* myThreadFun(void* vargp)
{
  sleep(10);
  printf("Sleep\n");
  return NULL;
}

int main(){
  map<string, tuple<int, int, int> > storage;


  map<int, map<string, vector<tuple<int, int> > > > dependency;



  int max_sd, sd, max_clients = 30, activity;

  int client_socket[30];
  for(int i=0;i<max_clients; i++){
    client_socket[i] = 0;
  }

  fd_set readfds;

  int server_fd, new_socket, valread;
  struct sockaddr_in address;
  int opt = 1;
  int addrlen = sizeof(address);
  char buffer[1024] = {0};
  char* hello = "Hello from server";

  //create socket file descriptor
  if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0){
    printf("socket failed");
}

  //Attach socket to port 8080
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))){
    printf("setsocktopt error");
      }

  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(PORT);

  if (bind(server_fd, (struct sockaddr *)&address, sizeof(address))<0){
    printf("bind failed");
  }



  printf("Listener on port %d\n", PORT);
  if (listen(server_fd, 3)<0){
    printf("listen error");
  }


  while(1){

    //clear the socket set
    FD_ZERO(&readfds);

    //add server socket to set
    FD_SET(server_fd, &readfds);
    max_sd = server_fd;

    //add child sockets to set
    for(int i=0; i<max_clients;i++){
      sd = client_socket[i];
      if(sd>0)
        FD_SET(sd, &readfds);
      if(sd>max_sd)
        max_sd = sd;
    }

    //wait for an activity on one of the sockets
    activity = select(max_sd+1, &readfds, NULL, NULL, NULL);

    if((activity <0) && (errno!=EINTR)){
        printf("select error");
    }


    //if something happened on the server socket, then its an incoming connection
    if(FD_ISSET(server_fd, &readfds)){

      if ((new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen))<0){
        printf("accept error");
      }

      printf("New connection, socket fd is %d, ip is %s, port is %d\n", new_socket, inet_ntoa(address.sin_addr), ntohs(address.sin_port));

      //add new socket to array of sockets
      for(int i=0; i<max_clients; i++){
        if(client_socket[i] == 0){
          client_socket[i] = new_socket;

          break;
        }
      }
    }



    //else its some IO operation on some other socket
    for(int i=0; i< max_clients; i++){
      sd = client_socket[i];

      if(FD_ISSET(sd, &readfds)){


        memset(buffer, 0, sizeof(buffer));
        valread = read(sd, buffer, 1024);

        char command[20];
        sscanf(buffer, "%s", command);
        int client_id = i;

        if(strcmp(command,"connect")==0){

          map<string, vector<tuple<int, int> > > context;
          dependency[client_id] = context;

          send(sd, "OK", strlen("OK"),0);
        }

        if(strcmp(command,"write")==0){
          string key;
          char tmp[20];
          int value;
          sscanf(buffer, "%s %s %d", command, tmp, &value);
          key = tmp;
          int timestamp = GlobalTime + 1;
          GlobalTime++;
          storage[key] = make_tuple(value, timestamp, my_id);

          // reply to client
          send(sd, "OK", strlen("OK"), 0);

          map< string, vector< tuple<int, int> > > nearest = dependency[client_id];
          dependency[client_id].clear();
          vector< tuple<int, int> > key_deps;
          key_deps.push_back( make_tuple(timestamp, my_id) );
          dependency[client_id][key] = key_deps;


          pthread_t thread_id;
          pthread_create(&thread_id, NULL, myThreadFun, NULL);
        }

        if(strcmp(command,"read")==0){
          string key;
          char tmp[20];
          sscanf(buffer, "%s %s", command, tmp);
          key = tmp;

          int value;
          int timestamp;
          value = get<0>(storage[key]);
          timestamp = get<1>(storage[key]);

          map<string, vector<tuple<int, int> > >::iterator it;
          if(it != dependency[client_id].find(key)){
            // when there already exists a key, we need to add more versions.
            dependency[client_id][key].push_back( make_tuple(timestamp, my_id) );
          }
          else{
            // create a vector to store the dependency of this key
            vector< tuple<int,int> > key_deps;
            key_deps.push_back( make_tuple(timestamp, my_id) );
            dependency[client_id][key] = key_deps;
          }

          char svalue[20];
          sprintf(svalue, "%d", value);
          send(sd, svalue, strlen(svalue), 0);
        }

        if(strcmp(command,"show_data")==0){
          cout<<"begin"<<endl;
          for(auto it = storage.begin(); it != storage.end(); ++it)
            cout<<it->first<<" => " << get<0>(it->second)<<endl;

          send(sd, "OK", strlen("OK"),0);

        }


        if(strcmp(command,"show_dep")==0){
          for(auto it = dependency.begin(); it != dependency.end(); ++it){
            cout<<"client "<<it->first<<":"<<endl;

            for(auto jt = it->second.begin(); jt != it->second.end(); ++jt){
              cout<<"key "<<jt->first<<":   ";
              for(auto kt = jt->second.begin(); kt != jt->second.end(); ++kt){
                cout<<get<0>(*kt)<<"  "<<get<1>(*kt)<<endl;
              }
            }
          }

          send(sd, "OK", strlen("OK"),0);

        }

        if(strcmp(command,"disconnect")==0){
          cout<<"Disconnect!\n";

          send(sd, "OK", strlen("OK"),0);

          dependency.erase(client_id);

          //close the socket
          close(sd);
          client_socket[i] = 0;

        }


      }

    }

  }

  return 0;
}
