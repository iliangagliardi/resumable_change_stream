# resumable_change_stream
The following is an example on how you can automatically implement a system where you process a change event and store the last processed token id into a special capped collection for the collection you're watching. 
There is also an example on how to start the change stream application at a specific point in time.

Python and Javascript are available. I will shortly produce a Java version.
