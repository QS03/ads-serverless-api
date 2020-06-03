# How to ...

### 1. Install Java

Please reference [here](https://www.oracle.com/java/technologies/javase-downloads.html).

### 2. Install Maven

Please reference [here](https://maven.apache.org/install.html)

### 3. Installing the AWS CLI

Please reference [here](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html)

### 4. Configuring the AWS CLI

```
$ aws configure --profile impakt
AWS Access Key ID [None]: < Your AWS Access Key ID here>
AWS Secret Access Key [None]:  < Your AWS Secret Access Key here>
Default region name [None]: us-east-1
Default output format [None]: json
```
Please reference [here](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html)

### 5. Install serverless framework

```
    npm install -g serverless
```
Please reference [here](https://www.serverless.com/framework/docs/providers/aws/guide/installation/)

### 6. Build

```json
  mvn clean install
```

### 7. Deploy

```json
  serverless deploy -v
```

### 8. Test

```json
  mvn test -Dtest=com.serverless.*
```
        
