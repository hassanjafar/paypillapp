def time(minutes,operation):
        from zk import ZK, const
        from datetime import datetime, timedelta

        zk = ZK('10.12.112.89', port=4370, timeout=5, password=0, force_udp=False, ommit_ping=False)
        conn = zk.connect()
        current=conn.get_time()
        dt=datetime.today()
        time_delta=datetime.now()-conn.get_time()
        timediff=time_delta.totalseconds()/60
        if time_delta >=10:
            print("ok")
        else:
            #actual=abs(time_delta+10-time_delta)
            actual=datetime.now()+timedelta(minutes=12)
            conn.set_time(actual)
            print(conn.get_time())
        #if operation =='gadaal':
        #    result = current - timedelta(minutes=minutes)
        #    conn.set_time(result)
        #    print("current time",conn.get_time())+
        #elif  operation =='horay':
        #    result = current + timedelta(minutes=minutes)
        #    conn.set_time(result)
        #    print("current time",conn.get_time())
