import gzip
import json
import os

pathz = "C:\\Users\\Thomas.Henshaw001\\Desktop\\SPOC Log"

#Takes an isoinstance and returns its approximate value in seconds since 0CE
def isoToFloat(isoInstance):
    indices = [0,4,5,7,8,10,11,13,14,16,17,26]
    parts = [isoInstance[i:j] for i,j in zip(indices, indices[1:]+[None])]
    timeNums = [x for x in parts if len(x)!=1]
    del timeNums[-1]
    floatTimes = [float(x) for x in timeNums]
    timeDial = [31536000,2592000,84600,3600,60,1]
    tick = 0
    timeFloat = 0
    for times in floatTimes:
        timeFloat += timeDial[tick]*times
        tick+=1
    return timeFloat

#Takes a path for a compressed log file and returns a digested dictionary of 'stop' and 'start' events 
def dayGzToDictionary(path):
    with gzip.open(path,'rb') as f:
        jsonLog = f.read().decode('utf-8')
        logList = str.splitlines(jsonLog)
        sessionList = []
        sessionDict = {}
        videoDict = {}
        bookmark = {}
        switch = 0
        startPhrase = ["play_video"]
        endPhrase = ['stop_video','pause_video','edx.video.paused','edx.video.played','edx.ui.lms.sequence.next_selected','edx.ui.lms.sequence.tab_selected','page_close','seek_video']
        eventFilter = startPhrase + endPhrase
        for log in logList:
            tempDict = json.loads(log)
            session = tempDict["session"]
            if session not in sessionList:
                sessionList.append(session)
            if tempDict["name"] in eventFilter:
                time_point = isoToFloat(tempDict['time'])  
                if tempDict["name"] in startPhrase:
                    event_type = "START"
                    event_details = json.loads(tempDict["event"])
                    vid_id = event_details["id"]
                    vid_duration = event_details["duration"]
                    if vid_id not in videoDict:
                        videoDict[vid_id] = vid_duration
                    if session not in sessionDict:
                        sessionDict[session] = {vid_id:{time_point:event_type}}
                    elif vid_id not in sessionDict[session]:
                        sessionDict[session][vid_id] = {time_point:event_type}
                    elif time_point not in sessionDict[session][vid_id]:
                        sessionDict[session][vid_id][time_point] = event_type
                    bookmark[session] = vid_id
                    switch = 1
                elif switch == 1:
                    event_type = "STOP"
                    switch = 0
                    sessionDict[session][bookmark[session]][time_point] = event_type
        videoWatchers = len(sessionDict.keys())
        totalSessions = len(sessionList)
        return sessionDict,videoDict,videoWatchers,totalSessions

#Aggregates dayGzToDictionary data    
def aggregateCrawl(directory):
    aggWatchSessions = {}
    aggVideoID = {}
    aggWatchers = 0
    aggSessions = 0
    logFiles = []
    for root, dirs, files in os.walk(os.path.abspath(directory)):
        for file in files:
            if file.endswith(".gz"):
                filePath = os.path.join(root, file)
                try:
                    a,b,c,d = dayGzToDictionary(filePath)
                    aggWatchSessions.update(a)
                    aggVideoID.update(b)
                    aggWatchers += c
                    aggSessions += d
                except:
                    ""
    return aggWatchSessions,aggVideoID,aggWatchers,aggSessions

def dataDigest(videoPlays,videoIDs,watchers,users):
    averages = []
    for sessions in videoPlays:
        for videos in videoPlays[sessions]:
            timePt = list(videoPlays[sessions][videos].keys())
            #event_type = list(videoPlays[sessions][videos].values())
            oddTimes = []
            evenTimes = []
            i = 0
            for times in timePt:
                if (i + 1) % 2 == 0:
                    oddTimes.append(times)
                else:
                    evenTimes.append(times)
                i+=1
            timeSlices = list(zip(oddTimes,evenTimes))
            differences = []
            for slices in timeSlices:
                difference = slices[0] - slices[1]
                differences.append(difference)
            timeWatched = sum(differences)
            fractionWatched = timeWatched / videoIDs[videos]
            averages.append((fractionWatched))
    print((sum(averages) / len(averages))*100, (watchers / users)*100)


            
a,b,c,d = aggregateCrawl(pathz)         
print(dataDigest(a,b,c,d))       
        
