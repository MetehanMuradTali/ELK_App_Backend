import time


def HpConfig(source,destination):
    resultHp = connection.send_command(f'rule deny tcp ${source} 10.1.2.0 0.0.0.255 ${destination} 10.1.1.0 0.0.0.255 tcp-flag syn')

    return {"status":resultHp["status"]}

class connection:

    def send_command(string):
        time.sleep(3) 
        return {"status":"Block"}