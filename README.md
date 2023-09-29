This application is an application that reads data from a kafka topic and process it before storing it into a new topic named "user-login-processed"

1. How would you deploy this application in production?

2. What other components would you want to add to make this production-ready?

3. How can this application scale with a growing dataset?

*To run this application in your local machine, please follow steps below:*
1. Pull all the files (except myenv if you are not tring to run the python script separately) into your machine.
2. Install Docker
3. In your terminal, type "docker compose up --build" and it should start running
4. Double confirm by checking the terminal messages and see if the .py script has produced success messages.