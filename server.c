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

#define R_IP "192.168.1.25"
#define R_PORT 8080

struct data_and_dependency {
  string key;
  int value;
  map< string, vector< tuple<int, int> > > client_dep;
};


void* sendReplicatedWrite(void* vargp)
{
  struct data_and_dependency *args = (struct data_and_dependency *) vargp;
  string key = args->key;
  int value = args->value;
  map< string, vector< tuple<int, int> > > client_dep = args->client_dep;


  sleep(5);

  int sock = 0, valread;
  struct sockaddr_in serv_addr;
  char buffer[1024] = {0};

  if((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0 ){
    printf("\n socket creation error\n");
    return NULL ;
  }

  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(R_PORT);

  if(inet_pton(AF_INET, R_IP, &serv_addr.sin_addr) <= 0){
    printf("invalid address\n");
    return NULL ;
  }

  if(connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0){
    printf("Connect failed\n");
    return NULL;
  }


  //prepare the data sent
  sprintf(buffer, "replicated_write ");
  sprintf(buffer + strlen(buffer),"%s ",key.c_str());
  sprintf(buffer + strlen(buffer),"%d ",value);

  int num_keys = client_dep.size();
  sprintf(buffer + strlen(buffer),"%d ", num_keys);
  for(auto it = client_dep.begin(); it != client_dep.end(); ++it){
    sprintf(buffer + strlen(buffer), "%s ",it->first.c_str());
    int num_deps_for_key = it->second.size();
    sprintf(buffer + strlen(buffer), "%d ", num_deps_for_key);
    for(auto jt = it->second.begin(); jt != it->second.end(); ++jt){
      sprintf(buffer + strlen(buffer), "%d %d ", get<0>(*jt), get<1>(*jt));
    }
  }


  send(sock, buffer, strlen(buffer), 0);


  delete args;
  return NULL;
}

int main(){
  char  test[200] = "hello world 333 hehe";
  char s[10],t[10],h[20];
  int d;
  char g[10];
  sscanf(test,"%s",s);
  sscanf(test+strlen(s)+1,"%s",t);
  sscanf(test+strlen(s)+strlen(t)+2, "%d", &d);
  sscanf(test+strlen(s)+strlen(t)+to_string(d).length() + 3, "%s", h);
  printf("%s %s %d %s\n", s,t,d,h);
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
          struct data_and_dependency *args = new data_and_dependency;
          args->key = key;
          args->value = value;
          args->client_dep = nearest;
          pthread_create(&thread_id, NULL, sendReplicatedWrite, args);
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


        if(strcmp(command, "replicated_write")==0){
          // deal with replicated_write request

          //parse the key, value, and dependency
          string key;
          char tmp[20];
          int index = 0;
          sscanf(buffer, "%s %s", command, tmp);
          index += strlen(command) + strlen(tmp) +2;
          key = tmp;

          int value;
          sscanf(buffer+index,"%d", &value);
          index += to_string(value).length() +1;

          map< string, vector< tuple<int, int> > > client_dep;

          int num_keys;
          sscanf(buffer+index,"%d", &num_keys);
          index += to_string(num_keys).length() +1;

          for(int j=0; j<num_keys; j++){
            string this_key;
            char tmp1[20];
            sscanf(buffer, "%s", tmp1);
            index += strlen(tmp1) + 1;
            this_key = tmp1;

            vector< tuple<int, int> > this_version_list;
            client_dep[this_key] = this_version_list;

            int num_deps_for_key;
            sscanf(buffer+index,"%d", &num_deps_for_key);
            index += to_string(num_deps_for_key).length() + 1;

            for(int k=0; k<num_deps_for_key; k++){
              int timestamp, server_id;
              sscanf(buffer+index,"%d %d", &timestamp, &server_id);
              index += to_string(timestamp).length() + to_string(server_id).length() + 2;
              this_version_list.push_back(make_tuple(timestamp, server_id));
            }
          }


          //print this client_dep
          for(auto it = client_dep.begin(); it!= client_dep.end(); it++){
            cout<<it->first<<": ";
            for(auto jt = it->second.begin(); jt!=it->second.end(); jt++){
              cout<<get<0>(*jt)<<"  "<<get<1>(*jt);
            }
            cout<<endl;
          }


        }
      }

    }

  }

  return 0;
}
