REM Script to start DPFT Engine

set PATH=%PATH%;D:\Program Files\java\java-1.8.0-openjdk-1.8.0.181-1
set EXEC_HOME=D:\ChannelEtl\Automation_Runtime
Rem java -XX:-UseGCOverheadLimit -Xmx16G -Xms16G -XX:PermSize=256m -XX:MaxPermSize=256m -XX:+UseG1GC -XX:+UseStringDeduplication -XX:ParallelGCThreads=20 -XX:MaxGCPauseMillis=200 -XX:ConcGCThreads=5 -XX:InitiatingHeapOccupancyPercent=50 -jar -Dcom.sun.management.jmxremote.port=3333 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dlog4j.configuration=file:%EXEC_HOME%\resources\log4j.properties %EXEC_HOME%\dpft-ext-0.0.1-SNAPSHOT.jar

REM java -Xmx12G -Xms4G -jar -Dlogging.config=D:\ChannelEtl\Automation_Runtime\config\logback-spring.xml -Dautomation.config=D:\ChannelEtl\Automation_Runtime\config\config.properties dpft-ext-0.0.1-SNAPSHOT.jar
REM jconsole �ʵ��[�Jport 8088
java -Xmx12G -Xms4G -jar -Dlogging.config=D:\ChannelEtl\Automation_Runtime\config\logback-spring.xml -Dautomation.config=D:\ChannelEtl\Automation_Runtime\config\config.properties dpft-ext-0.0.1-SNAPSHOT.jar
