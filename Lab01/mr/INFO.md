**Team Members:**
* Nicole Wang
  * Implemented worker-side logic
* Money Dhaliwal
  * Worked on coordinator and a little bit of worker side logic
  * Used ChatGPT to ask questions about Go (syntax, goroutines, data structures etc.) and understanding MapReduce concepts 
* Rahul Kamath
  * Implemented the Heartbeat on the Coordinator's Side that unassigns tasks from workers that are slow to complete them or stop pinging. Implemented the routine to ping the coordinator on the worker's side. Implemented the unique file naming scheme for outputs from map and reduce and conversion to expected output file names as well as related logic in the TaskComplete function.
  * AI Usage: The command `go build -buildmode=plugin` only works in unix based systems so I was using WSL2 to run my tests. However, this caused map tasks to take ~1 minute to finish which would cause the tests to timeout. Using **ChatGPT**, I was able to figure out that the issue was that I was mounting the windows C drive and running the project which caused file operations to be very slow. Transferring the project to the home directory of the VM solved the issue.

