import docker

client = docker.from_env()

# client.networks.create("network1",driver="bridge")

for i in client.containers.list():
    i.stop()

# rabbit = client.containers.run("rabbitmq:3-management",detach=True,ports={15672:15672,5672:5672})

# mongoContainer = client.containers.run('mongo',detach=True)
# worker = client.containers.run("worker",command=['python','worker.py','1'],links={mongoContainer.id:"mongodb",rabbit.id:"rabbitmq"},restart_policy={"Name":"on-failure"},detach=True)

# mongoContainer = client.containers.run('mongo',detach=True)
# worker = client.containers.run("worker",command=['python','worker.py','0']
#                                         ,links={mongoContainer.id:"mongodb",rabbit.id:"rabbitmq"}
#                                         ,restart_policy={"Name":"on-failure"}
#                                         ,detach=True)