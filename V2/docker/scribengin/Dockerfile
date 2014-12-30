######################################################################################
#                                                                                    #
# To build: docker build -t ubuntu:scribengin directory                              # 
# To run:   docker run -t -i ubuntu:scribengin  /bin/bash                            #
#                                                                                    #
######################################################################################
FROM ubuntu:14.04
#FROM ubuntu:latest
MAINTAINER Tuan Nguyen <tuan@demandcube.com>

#Update and installl the dependencies
RUN echo "Update and install the requirement software"
RUN apt-get update 
RUN apt-get install -y wget openssh-server openssh-client vim
RUN apt-get install -y openjdk-7-jdk

#Configure root Account
RUN mkdir /var/run/sshd
RUN echo 'root:root' | chpasswd

RUN sed -i 's/PermitRootLogin without-password/PermitRootLogin yes/' /etc/ssh/sshd_config

# SSH login fix. Otherwise user is kicked off after login
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd

ENV NOTVISIBLE "in users profile"
RUN echo "export VISIBLE=now" >> /etc/profile

#ADD authorized_keys /opt/authorized_keys
RUN mkdir -p /root/.ssh
RUN chmod 700 /root/.ssh
ADD tmp/authorized_keys /root/.ssh/authorized_keys
RUN chmod 644 /root/.ssh/authorized_keys

#Configure neverwinterdp account
RUN echo "Create neverwinterdp user account and setup home directory"

#ENV HOME /home/neverwinterdp
#RUN useradd -m -d /home/neverwinterdp -s /bin/bash -c "neverwinterdp user" -p $(openssl passwd -1 neverwinterdp)  neverwinterdp
#RUN echo "neverwinterdp ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

#A bug with file creation permission, need to create a template ssh configuration and copy to the account in the post installation
RUN mkdir -p        /opt/ssh-config
ADD tmp/id_rsa*     /opt/ssh-config/
RUN cat             /opt/ssh-config/id_rsa.pub > /opt/ssh-config/authorized_keys
#RUN chmod   600     /home/neverwinterdp/.ssh/id_rsa
#RUN chmod   600     /home/neverwinterdp/.ssh/id_rsa.pub
RUN chmod -R 700    /opt/ssh-config && chmod 644 /opt/ssh-config/authorized_keys
#RUN chgrp -R  neverwinterdp /home/neverwinterdp/.ssh

EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]

#Download java opensource projects
RUN wget -q -O - http://mirrors.digipower.vn/apache/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz | tar -xzf - -C /opt
RUN mv /opt/zookeeper-3.4.6 /opt/zookeeper
RUN cp /opt/zookeeper/conf/zoo_sample.cfg /opt/zookeeper/conf/zoo.cfg

RUN wget -q http://apache.01link.hk/kafka/0.8.1.1/kafka_2.9.2-0.8.1.1.tgz -O /tmp/kafka_2.9.2-0.8.1.1.tgz
RUN tar xfz /tmp/kafka_2.9.2-0.8.1.1.tgz -C /opt
RUN mv /opt/kafka_2.9.2-0.8.1.1 /opt/kafka

RUN wget -q  http://mirrors.digipower.vn/apache/hadoop/common/hadoop-2.6.0/hadoop-2.6.0.tar.gz -O /tmp/hadoop-2.6.0.tar.gz
RUN tar xfz /tmp/hadoop-2.6.0.tar.gz -C /opt
RUN mv /opt/hadoop-2.6.0 /opt/hadoop



#Run Post installation
ADD bootstrap/post-install.sh /opt/post-install.sh
RUN chmod +x /opt/post-install.sh
RUN /opt/post-install.sh
