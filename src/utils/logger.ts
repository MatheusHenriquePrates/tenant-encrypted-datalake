import pino from 'pino';
import { existsSync, mkdirSync } from 'node:fs';

const LOG_DIR = process.env['LOG_DIR'] || './logs';
const LOG_LEVEL = process.env['LOG_LEVEL'] || 'info';

if (!existsSync(LOG_DIR)) {
  mkdirSync(LOG_DIR, { recursive: true });
}

export const logger = pino({
  level: LOG_LEVEL,
  transport: {
    targets: [
      {
        target: 'pino/file',
        options: { destination: `${LOG_DIR}/datalake.log`, mkdir: true },
        level: LOG_LEVEL,
      },
      {
        target: 'pino-pretty',
        options: { colorize: true },
        level: LOG_LEVEL,
      },
    ],
  },
});

export function createChildLogger(module: string) {
  return logger.child({ module });
}
