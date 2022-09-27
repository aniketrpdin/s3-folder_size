const AWS = require("aws-sdk");

const REGION = "us-east-1";
const AWS_S3_ACCESS_KEY = "*****************";
const AWS_S3_KEY_SECRET = "***************";
const AWS_S3_BUCKET = "********";
// Create an Amazon S3 service client object.
AWS.config.update({
  accessKeyId: AWS_S3_ACCESS_KEY,
  secretAccessKey: AWS_S3_KEY_SECRET,
  region: REGION,
});

const s3 = new AWS.S3();

const bucketParams = { Bucket: AWS_S3_BUCKET };

const listSubFoldersOfAFolder = async function (folderPrefix) {
  let truncated = true;
  let pageMarker;
  const folders = [];

  if (folderPrefix) {
    bucketParams.Prefix = folderPrefix;
  }

  while (truncated) {
    try {
      const response = await s3.listObjects(bucketParams).promise();
      truncated = response.IsTruncated;
      response.Contents.forEach((item) => {
        const subFolderPath = item.Key.split(`${folderPrefix}/`);
        const isSubFolder=subFolderPath[1].split('/').length>2?true:false;
        if(isSubFolder){
            const subFolderName = subFolderPath[1].split('/')[0];
            if(folders.find((f)=>f===subFolderName)===undefined){
                folders.push(subFolderName);
            }
        }
      });

      if (truncated) {
        pageMarker = response.Contents.slice(-1)[0].Key;
        bucketParams.Marker = pageMarker;
      }
    } catch (error) {
      console.log(error);
      truncated = false;
    }
  }

  console.log({ folders });
};

//listSubFoldersOfAFolder("DIGITALASSET/2022");

const listAllFolders = async function (folderPrefix) {
  let truncated = true;
  let pageMarker;
  const files = [];

  if (folderPrefix) {
    bucketParams.Prefix = folderPrefix;
  }

  while (truncated) {
    try {
      const response = await s3.listObjects(bucketParams).promise();
      truncated = response.IsTruncated;
      response.Contents.forEach((item) => {
        const splitIndex = item.Key.search("/");
        if (splitIndex != -1) {
          const folderName = item.Key.slice(0, splitIndex);
          if (
            folderName &&
            files.find((file) => file === folderName) === undefined
          ) {
            files.push(folderName);
          }
        }
      });

      if (truncated) {
        pageMarker = response.Contents.slice(-1)[0].Key;
        bucketParams.Marker = pageMarker;
      }
    } catch (error) {
      console.log(error);
      truncated = false;
    }
  }

  console.log(files);
};
//listAllFolders();

const getFolderSize = async function(folderPrefix){
  let truncated = true;
  let pageMarker;
  let size =BigInt(0);
  if (folderPrefix) {
    bucketParams.Prefix = folderPrefix;
  }

  while (truncated) {
    try {
      const response = await s3.listObjects(bucketParams).promise();
      truncated = response.IsTruncated;
      response.Contents.forEach((item) => {
        size = BigInt(item.Size) + BigInt(size)
      });

      if (truncated) {
        pageMarker = response.Contents.slice(-1)[0].Key;
        bucketParams.Marker = pageMarker;
      }
    } catch (error) {
      console.log(error);
      truncated = false;
    }
  }
  console.log("Folder Name: ",folderPrefix)
  console.table([{
    kb:parseInt(size)/1024,
    mb:parseInt(size)/(1024*1024),
    gb:parseInt(size)/(1024*1024*1024)
  }])
}

getFolderSize("DIGITALASSET/2022/09")

const run = async function () {
  let truncated = true;
  let pageMarker;
  while (truncated) {
    try {
      const response = await s3.listObjects(bucketParams).promise();
      // return response; //For unit tests
      response.Contents.forEach((item) => {
        console.log(item.Key);
      });
      // Log the key of every item in the response to standard output.
      truncated = response.IsTruncated;
      // If truncated is true, assign the key of the last element in the response to the pageMarker variable.
      if (truncated) {
        pageMarker = response.Contents.slice(-1)[0].Key;
        // Assign the pageMarker value to bucketParams so that the next iteration starts from the new pageMarker.
        bucketParams.Marker = pageMarker;
      }
      // At end of the list, response.truncated is false, and the function exits the while loop.
    } catch (err) {
      console.log("Error", err);
      truncated = false;
    }
  }
};
