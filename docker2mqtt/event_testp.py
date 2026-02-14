import docker

## Create a Docker client
client = docker.from_env()


## Function to handle events
def handle_event(event):
    print(f"Event Type: {event['Type']}")
    print(f"Event Action: {event['Action']}")
    print(f"Event Actor: {event['Actor']}")
    print("---")


## Subscribe to Docker events
for event in client.events(decode=True, filters={"type": "container"}):
    handle_event(event)
