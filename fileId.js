const cluster = require("cluster");
const { cpus } = require("os");
const fs = require("fs");
const crypto = require("crypto");
const path = require("path");
const xlsx = require("xlsx");
const { homedir } = require("os");
const desktopDir = `${homedir()}/Desktop`;

if (cluster.isMaster) {
  const folderPath = "C:/Users/robin/Desktop/images"; // 文件夹路径
  const numCPUs = cpus().length; // CPU 核心数
  // const startTime = Date.now(); // 记录开始时间
  let fileIds = [];

  // 递归读取文件夹下的所有文件和子文件夹
  const items = readFolderSync(folderPath);

  // 将文件列表分批次分配给多个工作线程处理
  const batchSize = 20000; // 每批次处理的文件数
  const numBatches = Math.ceil(items.length / batchSize);
  let numProcessed = 0;

  for (let i = 0; i < numBatches; i++) {
    const batchStart = i * batchSize;
    const batchEnd = (i + 1) * batchSize;
    const batchItems = items.slice(batchStart, batchEnd);

    // 将批次文件列表分配给多个工作线程处理
    const numPerWorker = Math.ceil(batchItems.length / numCPUs);
    let itemIndex = 0;

    for (let j = 0; j < numCPUs; j++) {
      const worker = cluster.fork();
      const end = itemIndex + numPerWorker;
      const itemsToProcess = batchItems.slice(itemIndex, end);

      // 向工作线程发送要处理的文件列表
      worker.send({ itemsToProcess });
      itemIndex = end;

      // 处理工作线程完成的请求
      worker.on("message", (message) => {
        numProcessed += message.numProcessed;
        // console.log(`Processed ${numProcessed} files with worker ${worker.id}`);

        // console.log(message.fileIdArr);
        fileIds = [...fileIds, ...message.fileIdArr];

        // 如果所有文件都已处理完成，则输出总处理时间，且所有进程退出
        if (numProcessed === items.length) {
          // const endTime = Date.now();
          // console.log(`Processed all files in ${endTime - startTime} ms`);

          const wb = xlsx.utils.book_new();
          const sheetData = xlsx.utils.aoa_to_sheet([["fileName", "fileId"], ...fileIds]);
          xlsx.utils.book_append_sheet(wb, sheetData, "file_ids");
          xlsx.writeFile(wb, `${desktopDir}/file_ids.xlsx`);

          for (const id in cluster.workers) {
            cluster.workers[id].kill();
          }
        }
      });
    }
  }

  // 监听所有 Worker 进程的退出事件，如果所有进程都退出，则退出主进程
  let numExited = 0;
  for (const id in cluster.workers) {
    cluster.workers[id].on("exit", () => {
      numExited++;
      if (numExited === numCPUs) {
        process.exit();
      }
    });
  }
} else {
  // 监听父进程发来的要处理的文件列表，处理文件，并向父进程发送处理结果
  process.on("message", (data) => {
    const { itemsToProcess, wb } = data;

    let numProcessed = 0;
    let fileIdArr = [];
    itemsToProcess.forEach((item) => {
      if (item.isFile) {
        const fileData = fs.readFileSync(item.path);

        // sha256
        const hash = crypto.createHash("SHA256");
        hash.update(fileData);
        const fileId = hash.digest("hex");

        // xlsx.utils.book_append_sheet
        fileIdArr.push([item.name, fileId]);

        // console.log(`File ${item.name} has the ID ${fileId}`);
        numProcessed++;
      }
    });

    // 向父进程发送处理结果
    process.send({ numProcessed, fileIdArr });
  });
}

// 递归读取文件夹下的所有文件和子文件夹，返回文件列表
// 递归读取文件夹下的所有文件，返回文件列表
function readFolderSync(folderPath) {
  const items = [];

  fs.readdirSync(folderPath).forEach((item) => {
    const itemPath = path.join(folderPath, item);
    const stat = fs.statSync(itemPath);

    if (stat.isFile()) {
      items.push({ name: item, path: itemPath, isFile: true });
    } else if (stat.isDirectory()) {
      items.push(...readFolderSync(itemPath));
    }
  });

  return items;
}
