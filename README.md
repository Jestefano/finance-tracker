# General
This project creates a Telegram Bot that stores and retrieves personal finance data.
It has two main purposes:
1. As a finance project. I expect this project to help me gain control over my expenses.
2. As a technology project: I expect to use AWS core services to practice my current skills.

# Why? (Expected functionalities)
- Answer the question "How much money can I spend for the rest of the month (in non-recurrent expenses) without hurting my financial goals?"
- Being able to analyze last month expenses and detect undesired expense patterns.
- Being able to detect "lifestyle inflation"

# To-do
- Athena IAM

- Basic functionality
    - Deploy Lambda
    - Validar restriccion de IDs
    - Como mantener una "sesion" de maximo 3 minutos (para no hacer multiples calls, salvo que esto sea rapido)
    - Beautify el show
    - Anadir algo como show para ver todas categorias + tarjetas
    - Remaining of the month for extras
    - Clean up code
- Dashboard:
    - Streamlit
- Host 
    - AWS Lambda Dashboard
- Keep github updated

# Done
- If I add a new date, it automatically asks if there is a partition. If there isn't, it creates one for it.
- Clean S3 after any athena query

# Resources
- Telegram
    - Telegram documentation
    - https://www.freecodecamp.org/news/how-to-create-a-telegram-bot-using-python/
    - https://stackoverflow.com/questions/46015319/how-to-make-a-private-telegram-bot-accessible-only-by-its-owner

- https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html

- Athena in boto3
    - https://stackoverflow.com/questions/46844263/writing-json-to-file-in-s3-bucket
    - https://stackoverflow.com/questions/52026405/how-to-create-dataframe-from-aws-athena-using-boto3-get-query-results-method
    - https://medium.com/swlh/add-newly-created-partitions-programmatically-into-aws-athena-schema-d773722a228e
    - https://stackoverflow.com/questions/67424720/boto3-python-check-if-directory-exists-in-s3
    - https://stackoverflow.com/questions/59026551/s3-delete-files-inside-a-folder-using-boto3

- Id restriction
    - https://stackoverflow.com/questions/35368557/how-to-limit-access-to-a-telegram-bot

- Git/Github
    - https://www.theserverside.com/blog/Coffee-Talk-Java-News-Stories-and-Opinions/How-to-push-an-existing-project-to-GitHub

- Automatic deployment:
    - https://docs.timescale.com/timescaledb/latest/tutorials/aws-lambda/continuous-deployment/

- Streamlit in AWS
    - https://aws.amazon.com/es/blogs/opensource/using-streamlit-to-build-an-interactive-dashboard-for-data-analysis-on-aws/

- Requirements
    - https://stackoverflow.com/questions/31684375/automatically-create-requirements-txt

- Deploy Lambda
    - https://aws.plainenglish.io/develop-your-telegram-chatbot-with-aws-api-gateway-dynamodb-lambda-functions-410dcb1fb58a
    - https://www.youtube.com/watch?v=oYMgw4M4cD0

- Locally develop Lambda
    - https://www.youtube.com/watch?v=3BH79Uciw5w&t=238s
    - https://medium.com/claranet-italia/a-practical-guide-surviving-aws-sam-part-3-lambda-layers-8a55eb5d2cbe