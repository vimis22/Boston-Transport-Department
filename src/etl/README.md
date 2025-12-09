Inspired by: https://github.com/stackabletech/spark-k8s-operator/blob/0210fb9854755d2b1180b9c509b18aa2fbe2cb30/tests/templates/kuttl/spark-connect/20-run-connect-client.yaml.j2

Note, these etl flows cannot be run locally due to kafka's advertised listeners being incompatible with kubernetes port forwarding.
Instead, they should be run using docker build and kubectl apply -f job.yaml

You can also use VSCode run function, in the top right corner of the file, to run the job.
