Inspired by: https://github.com/stackabletech/spark-k8s-operator/blob/0210fb9854755d2b1180b9c509b18aa2fbe2cb30/tests/templates/kuttl/spark-connect/20-run-connect-client.yaml.j2

Note, these etl flows cannot be run locally due to kafka's advertised listeners being incompatible with kubernetes port forwarding.
Instead, they should be run using `docker build -t ghcr.io/vimis22/etl:x.y.z .`, and then updating the terraform in the etl module to use the new image.

You can also use VSCode run function, in the top right corner of the file, to run the job against the Jupyter kernel which is running in the kubernetes cluster.

