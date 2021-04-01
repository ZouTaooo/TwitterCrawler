nohup python3 -u FollowingTaskTcpServer.py >./log/FollowingTaskTcpServer.log &
sleep 3

nohup python3 -u FollowingTcpServer.py >./log/FollowingTcpServer.log &
sleep 3

nohup python3 -u MediaLinkTaskTcpServer.py >./log/MediaLinkTaskTcpServer.log &
sleep 3

nohup python3 -u LinksTcpServer.py >./log/LinksTcpServer.log &
sleep 3

nohup python3 -u ImageDownloadServer.py >./log/ImageDownloadServer.log &
sleep 3
