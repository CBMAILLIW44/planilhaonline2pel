import express from 'express';
import http from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PORT = Number(process.env.PORT) || 3000;
const isProduction = process.env.NODE_ENV === 'production';

// Persistent storage paths
const DATA_DIR = path.resolve(__dirname, 'data');
const RADIOS_FILE = path.join(DATA_DIR, 'radios.json');
const HISTORICO_FILE = path.join(DATA_DIR, 'historico.json');
const DOCUMENTS_FILE = path.join(DATA_DIR, 'documents.json');
const AUDIT_FILE = path.join(DATA_DIR, 'audit.json');
const HOSTED_FILES_FILE = path.join(DATA_DIR, 'hosted_files.json');

// Ensure data directory exists
if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

// Helpers for reading and atomic writing
function readJson<T>(filePath: string, fallback: T): T {
  try {
    if (fs.existsSync(filePath)) {
      const content = fs.readFileSync(filePath, 'utf-8');
      if (content.trim()) {
        return JSON.parse(content);
      }
    }
  } catch (err) {
    console.error(`[Server] Erro ao ler arquivo ${filePath}:`, err);
  }
  return fallback;
}

function writeJsonAtomic<T>(filePath: string, data: T): void {
  try {
    const tempPath = `${filePath}.tmp.${Date.now()}`;
    fs.writeFileSync(tempPath, JSON.stringify(data, null, 2), 'utf-8');
    fs.renameSync(tempPath, filePath);
  } catch (err) {
    console.error(`[Server] Erro ao salvar arquivo ${filePath}:`, err);
  }
}

// In-memory cache + file sync
let radiosCache = readJson<any[]>(RADIOS_FILE, []);
let historicoCache = readJson<any[]>(HISTORICO_FILE, []);
let documentsCache = readJson<any[]>(DOCUMENTS_FILE, []);
let auditCache = readJson<any[]>(AUDIT_FILE, []);
let hostedFilesCache = readJson<any[]>(HOSTED_FILES_FILE, []);
let lastUpdatedTimestamp = new Date().toISOString();

const app = express();
app.use(express.json({ limit: '60mb' }));
app.use(express.urlencoded({ extended: true, limit: '60mb' }));

const server = http.createServer(app);

// WebSocket Server for instant bidirectional updates across all devices
const wss = new WebSocketServer({ server, path: '/ws' });

// SSE listeners set (fallback for networks blocking WebSockets)
const sseClients = new Set<express.Response>();

function broadcastSSE(message: object) {
  const data = `data: ${JSON.stringify(message)}\n\n`;
  for (const client of sseClients) {
    try {
      client.write(data);
    } catch {
      sseClients.delete(client);
    }
  }
}

function broadcastWs(message: object, excludeWs?: WebSocket) {
  const data = JSON.stringify(message);
  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN && client !== excludeWs) {
      try {
        client.send(data);
      } catch (err) {
        console.error('[WS] Erro ao enviar mensagem:', err);
      }
    }
  }
}

function broadcastAll(message: object, excludeWs?: WebSocket) {
  broadcastWs(message, excludeWs);
  broadcastSSE(message);
}

// Broadcast connected users count
function broadcastPresence() {
  const count = wss.clients.size + sseClients.size;
  broadcastAll({
    type: 'PRESENCE',
    count: Math.max(1, count),
    timestamp: new Date().toISOString()
  });
}

// WebSocket Connection handling
wss.on('connection', (ws) => {
  // Send initial full state immediately upon connection
  ws.send(JSON.stringify({
    type: 'INIT',
    payload: {
      radios: radiosCache,
      historico: historicoCache,
      documents: documentsCache,
      auditLogs: auditCache,
      hostedFiles: hostedFilesCache,
      clientsCount: Math.max(1, wss.clients.size),
      lastUpdated: lastUpdatedTimestamp
    }
  }));

  broadcastPresence();

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw.toString());
      if (msg.type === 'PING') {
        ws.send(JSON.stringify({ type: 'PONG' }));
        return;
      }

      if (msg.type === 'SYNC_STATE') {
        const { radios, historico, reason, senderId, userMeta } = msg.payload || {};
        if (Array.isArray(radios)) {
          radiosCache = radios;
          writeJsonAtomic(RADIOS_FILE, radiosCache);
        }
        if (Array.isArray(historico)) {
          historicoCache = historico;
          writeJsonAtomic(HISTORICO_FILE, historicoCache);
        }
        lastUpdatedTimestamp = new Date().toISOString();

        // Automatically record to audit logs if userMeta/reason provided
        if (reason) {
          const newAudit: any = {
            id: `audit-${Date.now()}-${Math.random().toString(36).substring(2, 7)}`,
            dataHora: new Date().toISOString(),
            usuarioId: userMeta?.id || 'usr-op',
            usuarioNome: userMeta?.nome || 'Operador Militar',
            usuarioPosto: userMeta?.postoGraduacao || 'Militar',
            acao: reason.split(':')[0] || 'Modificação na Planilha',
            detalhes: reason,
            tipo: reason.toLowerCase().includes('cautela') ? 'cautela' 
                : reason.toLowerCase().includes('devol') ? 'devolucao' 
                : reason.toLowerCase().includes('exclu') ? 'sistema' : 'radio'
          };
          auditCache = [newAudit, ...auditCache].slice(0, 500);
          writeJsonAtomic(AUDIT_FILE, auditCache);
          broadcastAll({
            type: 'AUDIT_LOG_ADDED',
            log: newAudit,
            timestamp: new Date().toISOString()
          });
        }

        broadcastAll({
          type: 'STATE_UPDATED',
          payload: {
            radios: radiosCache,
            historico: historicoCache,
            reason: reason || 'Atualização de inventário',
            senderId,
            userMeta,
            lastUpdated: lastUpdatedTimestamp
          }
        }, ws);
      }
    } catch (err) {
      console.error('[WS] Erro no processamento de mensagem:', err);
    }
  });

  ws.on('close', () => {
    broadcastPresence();
  });
});

// Heartbeat ping interval every 25s
const pingInterval = setInterval(() => {
  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.ping();
    }
  }
}, 25000);

wss.on('close', () => {
  clearInterval(pingInterval);
});

// REST API Endpoints
// 1. Current State
app.get('/api/state', (_req, res) => {
  res.json({
    success: true,
    radios: radiosCache,
    historico: historicoCache,
    documents: documentsCache,
    auditLogs: auditCache,
    hostedFiles: hostedFilesCache,
    connectedClients: Math.max(1, wss.clients.size + sseClients.size),
    lastUpdated: lastUpdatedTimestamp
  });
});

// 2. State Sync Mutation (Updates JSON files and broadcasts in real-time)
app.post('/api/radios/sync', (req, res) => {
  const { radios, historico, reason, senderId, userMeta } = req.body || {};

  let changed = false;
  if (Array.isArray(radios)) {
    radiosCache = radios;
    writeJsonAtomic(RADIOS_FILE, radiosCache);
    changed = true;
  }

  if (Array.isArray(historico)) {
    historicoCache = historico;
    writeJsonAtomic(HISTORICO_FILE, historicoCache);
    changed = true;
  }

  if (changed) {
    lastUpdatedTimestamp = new Date().toISOString();

    // Automatically record to audit logs
    if (reason) {
      const newAudit: any = {
        id: `audit-${Date.now()}-${Math.random().toString(36).substring(2, 7)}`,
        dataHora: new Date().toISOString(),
        usuarioId: userMeta?.id || 'usr-op',
        usuarioNome: userMeta?.nome || 'Operador Militar',
        usuarioPosto: userMeta?.postoGraduacao || 'Militar',
        acao: reason.split(':')[0] || 'Modificação na Planilha',
        detalhes: reason,
        tipo: reason.toLowerCase().includes('cautela') ? 'cautela' 
            : reason.toLowerCase().includes('devol') ? 'devolucao' 
            : reason.toLowerCase().includes('exclu') ? 'sistema' : 'radio'
      };
      auditCache = [newAudit, ...auditCache].slice(0, 500);
      writeJsonAtomic(AUDIT_FILE, auditCache);
      broadcastAll({
        type: 'AUDIT_LOG_ADDED',
        log: newAudit,
        timestamp: new Date().toISOString()
      });
    }

    // Broadcast to all other devices in real-time
    broadcastAll({
      type: 'STATE_UPDATED',
      payload: {
        radios: radiosCache,
        historico: historicoCache,
        reason: reason || 'Atualização de inventário',
        senderId,
        userMeta,
        lastUpdated: lastUpdatedTimestamp
      }
    });
  }

  res.json({
    success: true,
    lastUpdated: lastUpdatedTimestamp,
    radiosCount: radiosCache.length,
    historicoCount: historicoCache.length
  });
});

// 3. Document Save
app.post('/api/documents', (req, res) => {
  const doc = req.body;
  if (!doc || !doc.id) {
    res.status(400).json({ error: 'Documento inválido' });
    return;
  }

  const existingIdx = documentsCache.findIndex((d) => d.id === doc.id);
  if (existingIdx >= 0) {
    documentsCache[existingIdx] = doc;
  } else {
    documentsCache.unshift(doc);
  }

  writeJsonAtomic(DOCUMENTS_FILE, documentsCache);
  broadcastAll({
    type: 'DOCUMENT_ADDED',
    doc,
    senderId: req.body.senderId,
    timestamp: new Date().toISOString()
  });

  res.json({ success: true, docId: doc.id });
});

// 4. Document Delete
app.delete('/api/documents/:id', (req, res) => {
  const { id } = req.params;
  documentsCache = documentsCache.filter((d) => d.id !== id);
  writeJsonAtomic(DOCUMENTS_FILE, documentsCache);

  broadcastAll({
    type: 'DOCUMENT_DELETED',
    id,
    timestamp: new Date().toISOString()
  });

  res.json({ success: true });
});

// 5. Audit Logs API
app.get('/api/audit', (_req, res) => {
  res.json({
    success: true,
    logs: auditCache
  });
});

app.post('/api/audit', (req, res) => {
  const entry = req.body;
  if (!entry || !entry.acao) {
    res.status(400).json({ error: 'Log inválido' });
    return;
  }

  const newLog = {
    id: entry.id || `audit-${Date.now()}-${Math.random().toString(36).substring(2, 7)}`,
    dataHora: entry.dataHora || new Date().toISOString(),
    usuarioId: entry.usuarioId || 'usr-anon',
    usuarioNome: entry.usuarioNome || 'Operador',
    usuarioPosto: entry.usuarioPosto || 'Militar',
    acao: entry.acao,
    detalhes: entry.detalhes || '',
    tipo: entry.tipo || 'sistema'
  };

  auditCache = [newLog, ...auditCache].slice(0, 500);
  writeJsonAtomic(AUDIT_FILE, auditCache);

  broadcastAll({
    type: 'AUDIT_LOG_ADDED',
    log: newLog,
    timestamp: new Date().toISOString()
  });

  res.json({ success: true, log: newLog });
});

app.delete('/api/audit', (_req, res) => {
  auditCache = [];
  writeJsonAtomic(AUDIT_FILE, auditCache);
  broadcastAll({
    type: 'AUDIT_LOG_CLEARED',
    timestamp: new Date().toISOString()
  });
  res.json({ success: true });
});

// 6. Hosted Files API (Aba Arquivos)
app.get('/api/files', (_req, res) => {
  res.json({
    success: true,
    files: hostedFilesCache
  });
});

app.post('/api/files', (req, res) => {
  const fileData = req.body;
  if (!fileData || !fileData.nomeOriginal || !fileData.arquivoUrl) {
    res.status(400).json({ error: 'Dados do arquivo incompletos.' });
    return;
  }

  const newFile = {
    id: fileData.id || `file-${Date.now()}-${Math.random().toString(36).substring(2, 7)}`,
    nomeOriginal: fileData.nomeOriginal,
    categoria: fileData.categoria || 'geral',
    tamanhoBytes: Number(fileData.tamanhoBytes) || 0,
    tipoMime: fileData.tipoMime || 'application/octet-stream',
    dataUpload: fileData.dataUpload || new Date().toISOString(),
    usuarioNome: fileData.usuarioNome || 'Operador',
    usuarioPosto: fileData.usuarioPosto || 'Militar',
    observacoes: fileData.observacoes || '',
    arquivoUrl: fileData.arquivoUrl
  };

  hostedFilesCache = [newFile, ...hostedFilesCache];
  writeJsonAtomic(HOSTED_FILES_FILE, hostedFilesCache);

  // Also record audit log for file upload
  const uploadAudit = {
    id: `audit-${Date.now()}-${Math.random().toString(36).substring(2, 7)}`,
    dataHora: new Date().toISOString(),
    usuarioId: fileData.usuarioId || 'usr-op',
    usuarioNome: newFile.usuarioNome,
    usuarioPosto: newFile.usuarioPosto,
    acao: 'Upload de Arquivo',
    detalhes: `Arquivo "${newFile.nomeOriginal}" (${(newFile.tamanhoBytes / 1024).toFixed(1)} KB) hospedado no repositório do site`,
    tipo: 'arquivo'
  };
  auditCache = [uploadAudit, ...auditCache].slice(0, 500);
  writeJsonAtomic(AUDIT_FILE, auditCache);

  broadcastAll({
    type: 'FILE_ADDED',
    file: newFile,
    timestamp: new Date().toISOString()
  });
  broadcastAll({
    type: 'AUDIT_LOG_ADDED',
    log: uploadAudit,
    timestamp: new Date().toISOString()
  });

  res.json({ success: true, file: newFile });
});

app.delete('/api/files/:id', (req, res) => {
  const { id } = req.params;
  const target = hostedFilesCache.find((f) => f.id === id);
  hostedFilesCache = hostedFilesCache.filter((f) => f.id !== id);
  writeJsonAtomic(HOSTED_FILES_FILE, hostedFilesCache);

  if (target) {
    const deleteAudit = {
      id: `audit-${Date.now()}-${Math.random().toString(36).substring(2, 7)}`,
      dataHora: new Date().toISOString(),
      usuarioId: 'usr-op',
      usuarioNome: 'Operador',
      usuarioPosto: 'Militar',
      acao: 'Exclusão de Arquivo',
      detalhes: `Arquivo "${target.nomeOriginal}" removido da hospedagem do site`,
      tipo: 'arquivo'
    };
    auditCache = [deleteAudit, ...auditCache].slice(0, 500);
    writeJsonAtomic(AUDIT_FILE, auditCache);
    broadcastAll({
      type: 'AUDIT_LOG_ADDED',
      log: deleteAudit,
      timestamp: new Date().toISOString()
    });
  }

  broadcastAll({
    type: 'FILE_DELETED',
    id,
    timestamp: new Date().toISOString()
  });

  res.json({ success: true });
});

// 7. Server-Sent Events (SSE) Stream
app.get('/api/events', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Access-Control-Allow-Origin': '*'
  });

  sseClients.add(res);
  broadcastPresence();

  // Initial event
  res.write(`data: ${JSON.stringify({
    type: 'INIT',
    payload: {
      radios: radiosCache,
      historico: historicoCache,
      documents: documentsCache,
      auditLogs: auditCache,
      hostedFiles: hostedFilesCache,
      lastUpdated: lastUpdatedTimestamp
    }
  })}\n\n`);

  req.on('close', () => {
    sseClients.delete(res);
    broadcastPresence();
  });
});

// 8. Complete Data Backup / Export
app.get('/api/backup', (_req, res) => {
  res.json({
    app: 'Controle de Rádios EB',
    exportedAt: new Date().toISOString(),
    radios: radiosCache,
    historico: historicoCache,
    documentsCount: documentsCache.length,
    auditLogsCount: auditCache.length,
    hostedFilesCount: hostedFilesCache.length,
    lastUpdated: lastUpdatedTimestamp
  });
});

// 9. System Health check
app.get('/api/health', (_req, res) => {
  res.json({
    status: 'online',
    system: 'EB Radio Control Real-Time Engine',
    connectedClients: Math.max(1, wss.clients.size + sseClients.size),
    radiosCount: radiosCache.length,
    historicoCount: historicoCache.length,
    documentsCount: documentsCache.length,
    auditLogsCount: auditCache.length,
    hostedFilesCount: hostedFilesCache.length,
    uptimeSeconds: Math.round(process.uptime()),
    timestamp: new Date().toISOString()
  });
});

// Vite Middleware or Static Assets Handler
async function setupFrontend() {
  const distPath = path.resolve(__dirname, 'dist');
  if (isProduction && fs.existsSync(distPath)) {
    console.log('[Server] Servindo arquivos de produção da pasta dist/');
    app.use(express.static(distPath));
    app.get('*', (_req, res) => {
      res.sendFile(path.resolve(distPath, 'index.html'));
    });
  } else {
    console.log('[Server] Montando Vite em modo dev middleware...');
    const { createServer: createViteServer } = await import('vite');
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: 'spa',
    });
    app.use(vite.middlewares);
  }

  server.listen(PORT, '0.0.0.0', () => {
    console.log(`[Servidor Operacional EB] Iniciado em http://0.0.0.0:${PORT}`);
    console.log(`[Sincronização em Tempo Real] WebSockets ativos na rota /ws`);
    console.log(`[Arquivos do Servidor] Armazenamento permanente em ${DATA_DIR}`);
  });
}

setupFrontend().catch((err) => {
  console.error('[Server] Falha fatal ao iniciar servidor:', err);
  process.exit(1);
});
