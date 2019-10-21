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
#include <algorithm>
#define PORT 8080

using namespace std;

int GlobalTime = 0;

int my_id = 0;

#define R_IP1 "104.39.68.199"
#define R_PORT1 8081
#define R_IP2 "104.39.68.199"
#define R_PORT2 8082

struct data_and_dependency {
  string key;
  int value;
  int interval1;
  int interval2;
  tuple<int, int> version;
  map< string, vector< tuple<int, int> > > client_dep;
};



void* checkDependency(void* vargp);


void* sendReplicatedWrite(void* vargp)
{


  struct data_and_dependency *args = (struct data_and_dependency *) vargp;
  string key = args->key;
  int value = args->value;
  int interval1 = args->interval1;
  int interval2 = args->interval2;
  tuple<int, int> version = args->version;
  map< string, vector< tuple<int, int> > > client_dep = args->client_dep;

  //prepare the data sent
  char buffer[1024] = {0};
  sprintf(buffer, "replicated_write ");
  sprintf(buffer + strlen(buffer),"%s ",key.c_str());
  sprintf(buffer + strlen(buffer),"%d ",value);
  sprintf(buffer + strlen(buffer),"%d %d ", get<0>(version), get<1>(version));

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


  printf("Send replicated write request to remote servers. key: %s, value: %d\n", key.c_str(), value);

  int sock1 = 0, sock2=0, valread;
  struct sockaddr_in serv_addr1;
  struct sockaddr_in serv_addr2;
  
  if((sock1 = socket(AF_INET, SOCK_STREAM, 0)) < 0 ){
    printf("\n socket creation error\n");
    return NULL ;
  }
  if((sock2 = socket(AF_INET, SOCK_STREAM, 0 )) < 0 ){
    printf("\n socket creation error\n");
    return NULL ;
  }

  serv_addr1.sin_family = AF_INET;
  serv_addr2.sin_family = AF_INET;
  
  if(interval1 <= interval2){
    sleep(interval1);
    serv_addr1.sin_port = htons(R_PORT1);
    
    if(inet_pton(AF_INET, R_IP1, &serv_addr1.sin_addr) <= 0){
    printf("invalid address\n");
    return NULL ;
    }

    if(connect(sock1, (struct sockaddr*)&serv_addr1, sizeof(serv_addr1)) < 0){
      printf("Connect failed\n");
      return NULL;
    }

    send(sock1, buffer, strlen(buffer), 0);

    sleep(interval2-interval1);
    serv_addr2.sin_port = htons(R_PORT2);

    if(inet_pton(AF_INET, R_IP2, &serv_addr2.sin_addr) <= 0){
      printf("invalid address\n");
      return NULL ;
    }

    if(connect(sock2, (struct sockaddr*)&serv_addr2, sizeof(serv_addr2)) < 0){
      printf("Connect failed\n");
      return NULL;
    }

    send(sock2, buffer, strlen(buffer), 0);

  }

  else{
    sleep(interval2);

    serv_addr2.sin_port = htons(R_PORT2);
    
    if(inet_pton(AF_INET, R_IP2, &serv_addr2.sin_addr) <= 0){
    printf("invalid address\n");
    return NULL ;
    }

    if(connect(sock2, (struct sockaddr*)&serv_addr2, sizeof(serv_addr2)) < 0){
      printf("Connect failed\n");
      return NULL;
    }

    send(sock2, buffer, strlen(buffer), 0);

    sleep(interval1-interval2);
    serv_addr1.sin_port = htons(R_PORT1);

    if(inet_pton(AF_INET, R_IP1, &serv_addr1.sin_addr) <= 0){
      printf("invalid address\n");
      return NULL ;
    }

    if(connect(sock1, (struct sockaddr*)&serv_addr1, sizeof(serv_addr1)) < 0){
      printf("Connect failed\n");
      return NULL;
    }

    send(sock1, buffer, strlen(buffer), 0);
  }


  delete args;
  return NULL;
}


//storage system: key -> value, timestamp, server_id
map<string, tuple<int, int, int> > storage;
//log history: key -> [timestamp, server_id, timestamp, server_id, ...,]
map<string, vector< tuple<int, int> > > histlog;
//pending dependency check lists: [data_and_dependency, data_and_dependency, ..., ]
vector< struct data_and_dependency > pending_check_list;

int main(){


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

          send(sd, "Connection built", strlen("Connection built"),0);
        }

        if(strcmp(command,"write")==0){
          string key;
          char tmp[20];
          int value;
	  int interval1;
	  int interval2;
          sscanf(buffer, "%s %s %d %d %d", command, tmp, &value, &interval1, &interval2);
          key = tmp;
          int timestamp = GlobalTime;
          GlobalTime++;
          storage[key] = make_tuple(value, timestamp, my_id);

          if(histlog.find(key) == histlog.end()){
            vector< tuple<int, int> > key_log;
            key_log.push_back(make_tuple(timestamp, my_id));
            histlog[key] = key_log;
          }
          else{
            histlog[key].push_back(make_tuple(timestamp, my_id));
          }



          // reply to client
	  char message[100];
	  printf("received request: %s\n", buffer);
	  sprintf(message, "%s is set to be %d\n", tmp, value); 
          send(sd, message, strlen(message), 0);

          map< string, vector< tuple<int, int> > > nearest = dependency[client_id];
          dependency[client_id].clear();
          vector< tuple<int, int> > key_deps;
          key_deps.push_back( make_tuple(timestamp, my_id) );
          dependency[client_id][key] = key_deps;


          pthread_t thread_id;
          struct data_and_dependency *args = new struct data_and_dependency;
          args->key = key;
          args->value = value;
	  args->interval1 = interval1;
	  args->interval2 = interval2;
          args->version = make_tuple(timestamp, my_id);
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
	  int server_id;
          value = get<0>(storage[key]);
          timestamp = get<1>(storage[key]);
	  server_id = get<2>(storage[key]);

          if(dependency[client_id].find(key) != dependency[client_id].end()){
            // when there already exists a key, we need to add more versions.
            dependency[client_id][key].push_back( make_tuple(timestamp, server_id) );
          }
          else{
            // create a vector to store the dependency of this key
            vector< tuple<int,int> > key_deps;
            key_deps.push_back( make_tuple(timestamp, server_id) );
            dependency[client_id][key] = key_deps;
          }

          char message[100];
	  printf("Received request: %s\n", buffer);
          sprintf(message, "the value of %s is %d\n", key.c_str(), value);
          send(sd, message, strlen(message), 0);
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
          cout<<"Received Disconnect Request!\n";

          send(sd, "Connection Closed", strlen("Connection Closed"),0);

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

          int timestamp, server_id;
          sscanf(buffer+index,"%d %d", &timestamp, &server_id);
          index += to_string(timestamp).length() + to_string(server_id).length() + 2;
          tuple<int, int> version = make_tuple(timestamp, server_id);

          cout<<"Received replicated request of key:"<<key<<" , value:"<<value<<endl;

          map< string, vector< tuple<int, int> > > client_dep;

          int num_keys;
          sscanf(buffer+index,"%d", &num_keys);
          index += to_string(num_keys).length() +1;

          for(int j=0; j<num_keys; j++){
            string this_key;
            char tmp1[20];
            sscanf(buffer+index, "%s", tmp1);
            index += strlen(tmp1) + 1;
            this_key = tmp1;

            vector< tuple<int, int> > this_version_list;

            int num_deps_for_key;
            sscanf(buffer+index,"%d", &num_deps_for_key);
            index += to_string(num_deps_for_key).length() + 1;


            for(int k=0; k<num_deps_for_key; k++){
              int timestamp, server_id;
              sscanf(buffer+index,"%d %d", &timestamp, &server_id);
              index += to_string(timestamp).length() + to_string(server_id).length() + 2;
              this_version_list.push_back(make_tuple(timestamp, server_id));

            }

            client_dep[this_key] = this_version_list;
          }



          //check if client_dep is satisfied or not
          int satisfied = 1;
          for(auto it = client_dep.begin(); it!= client_dep.end(); ++it){
            string this_key = it->first;
            auto temp = histlog.find(this_key);
            if(temp == histlog.end()){
              satisfied = 0;
              break;
            }
            vector< tuple<int, int> > this_key_log = histlog[this_key];
            for(auto jt = it->second.begin(); jt!=it->second.end(); ++jt){
              if( find(this_key_log.begin(), this_key_log.end(), make_tuple(get<0>(*jt),get<1>(*jt))) == this_key_log.end() ){
                satisfied = 0;
                break;
              }
            }
            if(satisfied == 0){
              break;
            }
          }

          if(satisfied == 0){
	    printf("Dependency check failed, delay it\n");
            //if dep check failed, add this client_dep to the pending_check_list
            struct data_and_dependency temp;
            temp.key = key;
            temp.value = value;
            temp.version = version;
            temp.client_dep = client_dep;
            pending_check_list.push_back(temp);
          }

          else{
	    printf("Dependency check passed, commit this replicated write request\n");
            // dep check passed, commit this replicated write request, add histlog info, update global time, issue new check for pending_check_list.
            storage[key] = make_tuple(value, get<0>(version), get<1>(version));


            if(histlog.find(key) == histlog.end()){
              vector< tuple<int, int> > key_log;
              key_log.push_back(make_tuple(get<0>(version), get<1>(version)));
              histlog[key] = key_log;
            }
            else{
              histlog[key].push_back(make_tuple(get<0>(version), get<1>(version)));
            }

            if (GlobalTime > get<0>(version)){
              GlobalTime ++;
            }
            else{
              GlobalTime = get<0>(version) + 1;
            }

            int loop = 1;
            while(loop){
	      printf("Reissue new check for potiential pending dependency\n");
              loop = 0;

              if(pending_check_list.size() == 0){
		printf("Pending check list is empty, we are done\n");
                loop = 0;
              }

              else{
                for(auto it = pending_check_list.begin(); it != pending_check_list.end(); ++it){
                  string pending_key = it->key;
                  int pending_value = it->value;
                  tuple<int, int> pending_version = it->version;
                  map< string, vector< tuple<int, int> > > pending_client_dep = it->client_dep;

                  satisfied = 1;
                  for(auto jt = pending_client_dep.begin(); jt!= pending_client_dep.end(); ++jt){
                    string this_key = jt->first;
                    auto temp = histlog.find(this_key);
                    if(temp == histlog.end()){
                      satisfied = 0;
                      break;
                    }
                    vector< tuple<int, int> > this_key_log = histlog[this_key];
                    for(auto kt = jt->second.begin(); kt != jt->second.end(); ++kt){
                      if( find(this_key_log.begin(), this_key_log.end(), make_tuple(get<0>(*kt),get<1>(*kt))) == this_key_log.end() ){
                        satisfied = 0;
                        break;
                      }
                    }
                    if(satisfied == 0){
                      break;
                    }
                  }

                  if(satisfied == 1){
		    printf("Pending check of key: %s satisfied, commit it\n", pending_key.c_str());
                    // dep check passed, commit this replicated write request, add histlog info, update global time, delete this dep from pending_check_list,  issue new check for pending_check_list
                    storage[pending_key] = make_tuple(pending_value, get<0>(pending_version), get<1>(pending_version));

                    if(histlog.find(pending_key) == histlog.end()){
                      vector< tuple<int ,int> > key_log;
                      key_log.push_back(make_tuple(get<0>(pending_version), get<1>(pending_version)));
                      histlog[pending_key] = key_log;
                    }
                    else{
                      histlog[pending_key].push_back(make_tuple(get<0>(pending_version), get<1>(pending_version)));
                    }

                    if (GlobalTime > get<0>(pending_version)){
                      GlobalTime ++;
                    }
                    else{
                      GlobalTime = get<0>(pending_version) + 1;
                    }

                    pending_check_list.erase(it);

                    loop = 1;

                    break;



                  }
                }
              }
            }
          }
        }
      }
    }
  }


  return 0;
}
