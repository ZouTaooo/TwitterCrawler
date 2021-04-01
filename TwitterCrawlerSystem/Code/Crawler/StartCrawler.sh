nohup python3 -u FollowingCrawler.py > ./log/FollowingCrawler.log &
sleep 3
nohup python3 -u MediaLinkCrawler.py > ./log/MediaLinkCrawler.log &
sleep 3
