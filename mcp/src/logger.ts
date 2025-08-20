/**
 * Simplified Logger with timestamp and log level formatting only
 */

/**
 * Logger levels with their corresponding numeric values
 */
enum LogLevel {
  DEBUG = 0,
  INFO = 1,
  WARN = 2,
  ERROR = 3,
  NONE = 100
}

/**
 * Parse log level from string
 */
const parseLogLevel = (level?: string): LogLevel | undefined => {
  if (!level) return undefined;
  const upperLevel = level.toUpperCase();
  switch (upperLevel) {
    case 'DEBUG': return LogLevel.DEBUG;
    case 'INFO': return LogLevel.INFO;
    case 'WARN': return LogLevel.WARN;
    case 'ERROR': return LogLevel.ERROR;
    case 'NONE': return LogLevel.NONE;
    default: return undefined;
  }
}

/**
 * Global log level configuration
 */
declare global {
  var logLevel: LogLevel;
}
global.logLevel = parseLogLevel(process.env.LOG_LEVEL) ?? LogLevel.INFO;

const _console = console
global.console = {
  ...global.console,
  log: (message?: any, ...optionalParams: any[]) => {
    shouldLog(LogLevel.INFO) && _console.log(message, ...optionalParams);
  },
  warn: (message?: any, ...optionalParams: any[]) => {
    shouldLog(LogLevel.WARN) && _console.warn(message, ...optionalParams);
  },
  error: (message?: any, ...optionalParams: any[]) => {
    shouldLog(LogLevel.ERROR) && _console.error(message, ...optionalParams);
  },
  debug: (message?: any, ...optionalParams: any[]) => {
    shouldLog(LogLevel.DEBUG) && _console.debug(message, ...optionalParams);
  },
};

const shouldLog = (level: LogLevel) => {
  return global.logLevel <= level
};
/**
 * Simplified Logger class with only timestamp and log level formatting
 */
class Logger {
  /**
   * Create a new Logger instance
   */
  constructor() {}

  /**
   * Format a log message with timestamp and level only
   * 
   * @param level Log level for this message
   * @param message The message to log
   * @param args Additional arguments to include
   * @returns Formatted log message
   */
  private formatMessage(level: string, message: string, args: any[] = []): string {
    const timestamp = `[${new Date().toISOString()}]`;
    const levelFormatted = `[${level.padEnd(5)}]`;
    
    let formattedMessage = `${timestamp} ${levelFormatted} ${message}`;

    if (args.length > 0) {
      const argsString = args.map(arg => String(arg)).join(' ');
      formattedMessage += ` ${argsString}`;
    }
    
    return formattedMessage;
  }

  /**
   * Log a debug message
   */
  debug(message: string, ...args: any[]): void {
    if (shouldLog(LogLevel.DEBUG)) {
      const formattedMessage = this.formatMessage('DEBUG', message, args);
      _console.log(formattedMessage);
    }
  }

  /**
   * Log an info message
   */
  info(message: string, ...args: any[]): void {
    if (shouldLog(LogLevel.INFO)) {
      const formattedMessage = this.formatMessage('INFO', message, args);
      _console.log(formattedMessage);
    }
  }

  /**
   * Log a warning message
   */
  warn(message: string, ...args: any[]): void {
    if (shouldLog(LogLevel.WARN)) {
      const formattedMessage = this.formatMessage('WARN', message, args);
      _console.warn(formattedMessage);
    }
  }

  /**
   * Log an error message
   */
  error(message: string, ...args: any[]): void {
    if (shouldLog(LogLevel.ERROR)) {
      const formattedMessage = this.formatMessage('ERROR', message, args);
      _console.error(formattedMessage);
    }
  }
}
  
export { Logger, LogLevel };
