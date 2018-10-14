cd ~/bigdata/hadoopConf/
for i in `cat {slaves,masters}`; do ssh $USER@$i 'rm -fr /s/$i/a/tmp/hadoop-$USER/*'; done
for i in `cat {slaves,masters}`; do ssh $USER@$i ' rm -fr /tmp/$USER/*'; done
#for i in `cat {slaves,masters}`; do ssh $USER@$i rm -fr /tmp/$USER/*; done
#for i in richmond salem; do ssh $USER@$i rm -fr /tmp/$USER/*; done
for i in salem richmond; do ssh $USER@$i 'rm -fr /s/$i/a/tmp/hadoop-$USER/* && rm -fr /tmp/$USER/*'; done
for i in providence tallahassee; do ssh $USER@$i 'rm -fr /s/$i/a/tmp/hadoop-$USER/*'; done
for i in providence tallahassee; do ssh $USER@$i 'rm -fr /tmp/$USER/*'; done
