FROM ubuntu:16.04

ADD test.csv /root/

RUN apt-get update && apt-get install -y openssh-server
RUN mkdir /var/run/sshd
# Password & Authentication
RUN echo 'root:admin123' | chpasswd
RUN sed -i 's/PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

# SSH & Keeping Session Alive
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd

ENV NOTVISIBLE "in users profile"
RUN echo "export VISIBLE=now" >> /etc/profile

EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]
