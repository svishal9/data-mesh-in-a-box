import jenkins.model.*
Jenkins.instance.setNumExecutors(0) // Recommended to not enable executors on the controller