const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

const axios = require('axios');
const fs = require('fs');
const cheerio = require('cheerio');
const http = require('http');
const https = require('https');

const PROVINCES = [
  'Banten', 
  'DKI Jakarta', 
  'Jawa Barat', 
  'Jawa Tengah', 
  'DI Yogyakarta', 
  'Jawa Timur'
];

const NUM_THREADS = 6; 
const LENGTH = 100;
const CONCURRENCY_LIMIT = 5; 
const PROGRESS_FILE = 'progress_jawa.json';

if (isMainThread) {
  (async () => {
    console.log('🔐 [Master] Mengambil session dari Puppeteer (Stealth Mode)...');
    
    const browser = await puppeteer.launch({ 
      headless: true,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-blink-features=AutomationControlled'
      ]
    });
    
    const page = await browser.newPage();
    await page.setViewport({ width: 1366, height: 768 });

    let gotoRetries = 3;
    while (gotoRetries > 0) {
      try {
        await page.goto('https://sirup.inaproc.id/sirup/caripaketctr/index', {
          waitUntil: 'networkidle2',
          timeout: 60000
        });
        break; 
      } catch (err) {
        gotoRetries--;
        console.log(`⚠️ [Master] Jaringan tidak stabil (${err.message}). Mencoba ulang... (Sisa: ${gotoRetries})`);
        if (gotoRetries === 0) {
          await browser.close();
          throw new Error("Gagal membuka halaman utama setelah 3 percobaan. Cek koneksi internetmu.");
        }
        await new Promise(r => setTimeout(r, 5000)); 
      }
    }

    await new Promise(r => setTimeout(r, 3000));

    const cookies = await page.cookies();
    const cookieString = cookies.map(c => `${c.name}=${c.value}`).join('; ');
    
    const userAgent = await page.evaluate(() => navigator.userAgent);
    
    await browser.close();
    console.log('✅ [Master] Session didapatkan. Memulai Worker...\n');

    let progress = {};
    if (fs.existsSync(PROGRESS_FILE)) {
      progress = JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf8'));
      console.log('📂 [Master] Ditemukan file progress sebelumnya. Melanjutkan scraping...\n');
    } else {
      PROVINCES.forEach(prov => progress[prov] = 0);
      fs.writeFileSync(PROGRESS_FILE, JSON.stringify(progress, null, 2));
    }

    const chunkSize = Math.ceil(PROVINCES.length / NUM_THREADS);
    const workerTasks = [];
    for (let i = 0; i < PROVINCES.length; i += chunkSize) {
      workerTasks.push(PROVINCES.slice(i, i + chunkSize));
    }

    for (let i = 0; i < NUM_THREADS; i++) {
      const workerId = i + 1;
      const assignedProvinces = workerTasks[i];

      if (!assignedProvinces || assignedProvinces.length === 0) continue;

      const worker = new Worker(__filename, {
        workerData: { workerId, assignedProvinces, cookieString, userAgent, progressFile: PROGRESS_FILE }
      });

      worker.on('message', (msg) => {
        if (msg.type === 'log') {
          console.log(msg.text);
        } else if (msg.type === 'progress') {
          progress[msg.province] = msg.currentStart;
          fs.writeFileSync(PROGRESS_FILE, JSON.stringify(progress, null, 2));
        }
      });

      worker.on('error', (err) => console.error(`❌ [Worker ${workerId}] Fatal Error:`, err));
      worker.on('exit', (code) => {
        if (code !== 0) console.error(`⚠️ [Worker ${workerId}] Berhenti dengan kode ${code}`);
        else console.log(`🎉 [Worker ${workerId}] PROVINSI SELESAI!`);
      });
    }
  })();
} 

else {
  const { workerId, assignedProvinces, cookieString, userAgent, progressFile } = workerData;
  const OUTPUT = `sirup_2026_jawa_worker${workerId}.csv`;

  const httpAgent = new http.Agent({ keepAlive: true, maxSockets: 100 });
  const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 100 });
  const axiosInstance = axios.create({ httpAgent, httpsAgent, timeout: 30000 });

  function sleep(ms) {
    return new Promise(r => setTimeout(r, ms));
  }

  function randomSleep(min, max) {
    const delay = Math.floor(Math.random() * (max - min + 1)) + min;
    return new Promise(r => setTimeout(r, delay));
  }

  function csvEscape(value) {
    if (!value) return '';
    return `"${String(value).replace(/"/g, '""')}"`;
  }

  function convertYaTidak(val) {
    if (val === true) return 'Ya';
    if (val === false) return 'Tidak';
    return val || '';
  }

  async function getDetail(id, retries = 3) {
    try {
      const url = `https://sirup.inaproc.id/sirup/rup/detailPaketPenyedia2020?idPaket=${id}`;
      const res = await axiosInstance.get(url, {
        headers: {
          "User-Agent": userAgent, 
          "Accept": "text/html,application/xhtml+xml",
          "X-Requested-With": "XMLHttpRequest",
          "Cookie": cookieString 
        }
      });

      const $ = cheerio.load(res.data);
      const getValue = (label) => $(`td.label-left:contains("${label}")`).next("td").text().trim().replace(/\s+/g, " ");
      const getRange = (label) => {
        const tds = $(`td.label-left:contains("${label}")`).next("td").find("td.mid");
        return tds.length >= 2 ? `${$(tds[0]).text().trim()} - ${$(tds[1]).text().trim()}` : "";
      };

      return {
        spesifikasi: getValue("Spesifikasi Pekerjaan"),
        pemanfaatan: getRange("Pemanfaatan Barang/Jasa"),
        kontrak: getRange("Jadwal Pelaksanaan Kontrak"),
        pemilihan: getRange("Jadwal Pemilihan Penyedia")
      };
    } catch (err) {
      if (retries > 0) {
        const errorType = err.response ? `Status ${err.response.status}` : err.code || err.message;
        parentPort.postMessage({ type: 'log', text: `⏳ [Worker ${workerId}] Gagal ID ${id} (${errorType}). Mencoba ulang... (Sisa retry: ${retries - 1})` });
        
        await sleep(5000);
        return getDetail(id, retries - 1); 
      }

      const reason = err.response ? `Status ${err.response.status}` : err.code || err.message;
      parentPort.postMessage({ type: 'log', text: `❌ [Worker ${workerId}] MENYERAH pada ID ${id} setelah retries habis: ${reason}` });
      return { spesifikasi: "", pemanfaatan: "", kontrak: "", pemilihan: "" };
    }
  }

  (async () => {
    const writeStream = fs.createWriteStream(OUTPUT, { flags: 'a' });
    if (!fs.existsSync(OUTPUT) || fs.statSync(OUTPUT).size === 0) {
      writeStream.write('provinsi_pencarian,no,paket,pagu,jenis_pengadaan,produk_dalam_negeri,usaha_kecil_koperasi,metode,pemilihan,klpd,satuan_kerja,lokasi,id,spesifikasi_pekerjaan,pemanfaatan_barang_jasa,jadwal_pelaksanaan_kontrak,jadwal_pemilihan_penyedia\n');
    }

    let currentProgress = JSON.parse(fs.readFileSync(progressFile, 'utf8'));

    for (const province of assignedProvinces) {
      let currentStart = currentProgress[province] || 0;
      parentPort.postMessage({ type: 'log', text: `🚀 [Worker ${workerId}] Memulai Provinsi: ${province} dari offset ${currentStart}` });

      let isFinished = false;

      while (!isFinished) {
        try {
          const res = await axiosInstance.get('https://sirup.inaproc.id/sirup/caripaketctr/search', {
            headers: {
              "Cookie": cookieString,
              "User-Agent": userAgent, 
              "Accept": "application/json, text/javascript, */*; q=0.01",
              "X-Requested-With": "XMLHttpRequest"
            },
            params: {
              tahunAnggaran: 2026, draw: 1, start: currentStart, length: LENGTH,
              'search[value]': province, 
              'columns[1][data]': 'paket', 'columns[2][data]': 'pagu',
              'columns[3][data]': 'jenisPengadaan', 'columns[4][data]': 'isPDN',
              'columns[5][data]': 'isUMK', 'columns[6][data]': 'metode',
              'columns[7][data]': 'pemilihan', 'columns[8][data]': 'kldi',
              'columns[9][data]': 'satuanKerja', 'columns[10][data]': 'lokasi',
              'columns[11][data]': 'id', 'order[0][column]': 5, 'order[0][dir]': 'DESC'
            }
          });

          if (typeof res.data === 'string' && res.data.includes('<html')) {
              throw new Error('Session Expired / Diblokir WAF');
          }

          const rows = res.data.data;

          if (!rows || rows.length === 0) {
            parentPort.postMessage({ type: 'log', text: `🏁 [Worker ${workerId}] Provinsi ${province} SELESAI di offset ${currentStart}.` });
            isFinished = true;
            break;
          }

          for (let i = 0; i < rows.length; i += CONCURRENCY_LIMIT) {
            const batch = rows.slice(i, i + CONCURRENCY_LIMIT);
            const details = await Promise.all(batch.map(row => getDetail(row.id)));

            let lines = '';
            batch.forEach((row, index) => {
              const detail = details[index];
              lines += [
                csvEscape(province), csvEscape(row.no), csvEscape(row.paket), csvEscape(row.pagu),
                csvEscape(row.jenisPengadaan), csvEscape(convertYaTidak(row.isPDN)),
                csvEscape(convertYaTidak(row.isUMK)), csvEscape(row.metode),
                csvEscape(row.pemilihan), csvEscape(row.kldi), csvEscape(row.satuanKerja),
                csvEscape(row.lokasi), csvEscape(row.id), csvEscape(detail.spesifikasi),
                csvEscape(detail.pemanfaatan), csvEscape(detail.kontrak), csvEscape(detail.pemilihan)
              ].join(',') + '\n';
            });
            writeStream.write(lines);
            
            await randomSleep(500, 1500);
          }

          currentStart += LENGTH;
          
          parentPort.postMessage({ type: 'progress', province: province, currentStart: currentStart });
          parentPort.postMessage({ type: 'log', text: `✅ [Worker ${workerId}] [${province}] Progress: offset ${currentStart}` });
          
          await randomSleep(2000, 4000); 

        } catch (err) {
          const errorMessage = err.response ? `Status ${err.response.status}` : err.message;
          parentPort.postMessage({ type: 'log', text: `❌ [Worker ${workerId}] [${province}] Error offset ${currentStart} (${errorMessage}), retrying dalam 15 detik...` });
          await sleep(15000); 
        }
      }
    }

    writeStream.end();
  })();
}