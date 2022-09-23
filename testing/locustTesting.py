import time
import random
import sys
from locust import HttpUser, TaskSet, task, between, run_single_user, events, constant

headerDictTrino = {'Content-Type': 'text/plain; charset=UTF-8',
                   'User-Agent': 'StatementClientV1/387',
                   'X-Trino-Source': 'trino-cli',
                   'Accept-Encoding': 'gzip',
                   'X-Trino-Language': 'en',
                   'X-Trino-User': 'dell',
                   'X-Trino-Transaction-Id': 'NONE',
                   'X-Trino-Time-Zone': 'America/New_York',
                   'X-Trino-Client-Tags': 'infratrino',
                   'X-Trino-Catalog': 'hive',
                   'X-Trino-Schema': '6sense'}

headerDictPresto = {'Content-Type': 'text/plain; charset=UTF-8',
                    'User-Agent': 'StatementClientV1/339',
                    'Connection': 'keep-alive',
                    'X-Presto-Source': 'presto-cli',
                    'Accept-Encoding': 'gzip',
                    'X-Presto-Language': 'en',
                    'X-Presto-Transaction-Id': 'NONE',
                    'X-Presto-User': 'rohanc',
                    'X-Presto-Time-Zone': 'America/New_York',
                    'X-Presto-Client-Tags': 'adhoc',
                    'X-Presto-Catalog': 'hive',
                    'X-Presto-Schema': 'dell',
                    'X-Presto-Client-Capabilities': 'PATH,PARAMETRIC_DATETIME'}

shortQueryList = ["show tables", "select count(*) from company_master_lookup"]

longQuery = 'select distinct {selection} from ds_activity_playground_mastered where data_source in (\'{source1}\', \'{source2}\')'
longQuerySelectionList = ["source_activity_name", "master_id"]
longQueryDataSource = ["map", "map_tt", "task", "task_tt", "web", "web_tt"]

tempLongQuery = 'select * from sfdc_task_raw limit 50000'
threeGBQuery = 'select distinct source_activity_name from ds_activity_playground_mastered where data_source in (\'map_tt\', \'web\')'


def randomShortQuery():
    return shortQueryList[random.randint(0, len(shortQueryList) - 1)]


def randomLongQuery():
    sources = random.sample(range(len(longQueryDataSource)), 2)
    return longQuery.format(selection=longQuerySelectionList[random.randint(0, len(longQuerySelectionList) - 1)],
                            source1=longQueryDataSource[sources[0]],
                            source2=longQueryDataSource[sources[1]])


class SimulatedUser(TaskSet):
    @task(4)
    def makeLongPrestoCall(self):
        self.makePrestoCall("Long")

    def makeShortPrestoCall(self):
        self.makePrestoCall("Short")

    def makePrestoCall(self, queryType):
        startTime = time.time()
        first = True
        response = None

        try:
            queryToUse = (tempLongQuery if queryType ==
                          "Long" else randomShortQuery())
            headerToUse = headerDictPresto

            headerToUse['Content-Length'] = str(len(queryToUse))

            response = self.client.post(
                "/v1/statement", data=queryToUse, headers=headerToUse, catch_response=True)

            dict = response.json()
            while ('nextUri' in dict):
                if response.status_code != 200:
                    raise Exception(
                        f"Non 200 response: {response.status_code} received")
                elif first:
                    totalTime = int((time.time() - startTime) * 1000)
                    events.request_success.fire(
                        request_type="Post Request", name=queryType, response_time=totalTime, response_length=0)
                    first = False

                timeBeforeGet = time.time()
                response = self.client.get(
                    dict['nextUri'], catch_response=True)

                for i in range(3):
                    if response.status_code - 500 >= 0:
                        print(f'Retry attempt {i} due to {response.status_code}')
                        response = self.client.get(
                            dict['nextUri'], catch_response=True)
                    else:
                        break

                getTime = int((time.time() - timeBeforeGet) * 1000)

                getNum = dict['nextUri'].split("/")[-1]

                if (int(getNum) > 1):
                    events.request_success.fire(
                        request_type="Get Request", name=queryType, response_time=getTime, response_length=sys.getsizeof(response.text))
                dict = response.json()

            totalTime = int((time.time() - startTime) * 1000)
            events.request_success.fire(
                request_type="Complete Request", name=queryType, response_time=totalTime, response_length=0)
        except Exception as e:
            totalTime = int((time.time() - startTime) * 1000)
            print(response.request)
            print(response.headers)
            print(response.text)
            print(response.status_code)
            print(response.json())

            if first:
                events.request_failure.fire(request_type="Post Request", name=queryType,
                                            response_time=totalTime, response_length=0, exception=e)

            events.request_failure.fire(request_type="Complete Request", name=queryType,
                                        response_time=totalTime, response_length=0, exception=e)


class MainClass(HttpUser):
    tasks = [SimulatedUser]
    wait_time = constant(1)


if __name__ == "__main__":
    run_single_user(MainClass)
