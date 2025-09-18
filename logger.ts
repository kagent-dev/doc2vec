/**
 * Enhanced Logger for structured and consistent logging
 * Compatible with CommonJS and ESM environments
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
 * Configuration options for the Logger
 */
interface LoggerConfig {
  useTimestamp: boolean;
  useColor: boolean;
  logToFile?: string;
  prettyPrint?: boolean;
}
  
/**
 * Basic color functions that don't rely on external packages
 */
const colors = {
  gray: (text: string) => `\x1b[90m${text}\x1b[0m`,
  blue: (text: string) => `\x1b[34m${text}\x1b[0m`,
  yellow: (text: string) => `\x1b[33m${text}\x1b[0m`,
  red: (text: string) => `\x1b[31m${text}\x1b[0m`,
  green: (text: string) => `\x1b[32m${text}\x1b[0m`,
  reset: (text: string) => `\x1b[0m${text}\x1b[0m`
};

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
 * Overwrite standard console Logger to only log messages at the configured log level
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
 * Enhanced Logger class with color-coding, timestamp, and formatting
 */
class Logger {
  private config: LoggerConfig;
  private moduleName: string;

  /**
   * Create a new Logger instance
   * 
   * @param moduleName Name of the module using this logger
   * @param config Logger configuration options
   */
  constructor(moduleName: string, config?: Partial<LoggerConfig>) {
    this.moduleName = moduleName;
    this.config = {
      useTimestamp: true,
      useColor: true,
      prettyPrint: true,
      ...config
    };
  }

  /**
   * Format a log message with timestamp, level, and module information
   * 
   * @param level Log level for this message
   * @param message The message to log
   * @param args Additional arguments to include
   * @returns Formatted log message
   */
  private formatMessage(level: string, message: string, args: any[] = []): string {
    const timestamp = this.config.useTimestamp ? 
      `[${new Date().toISOString()}] ` : '';
    
    const modulePrefix = this.moduleName ? 
      `[${this.moduleName}] ` : '';
    
    const levelFormatted = `[${level.padEnd(5)}]`;
    
    let formattedMessage = `${timestamp}${levelFormatted} ${modulePrefix}${message}`;

    if (args.length > 0) {
      const argsString = args.map(arg => {
        if (arg instanceof Error) {
          return `\n--- Error Details ---\nMessage: ${arg.message}\nStack:\n${arg.stack}\n--- End Error ---`;
        } 
        else if (this.config.prettyPrint && typeof arg === 'object' && arg !== null) {
          try {
            return JSON.stringify(arg, null, 2);
          } catch (e) {
            return "[Unserializable Object]"; 
          }
        } else {
          return String(arg); 
        }
      }).join('\n');

      if (this.config.prettyPrint) {
          formattedMessage += `\n${argsString}`; 
      } else {
          formattedMessage += ` ${args.map(String).join(' ')}`;
      }
    }
    
    return formattedMessage;
  }

  /**
   * Apply color to a message based on log level
   * 
   * @param level Log level
   * @param message Message to color
   * @returns Colored message
   */
  private colorize(level: LogLevel, message: string): string {
    if (!this.config.useColor) return message;
    
    switch (level) {
      case LogLevel.DEBUG:
        return colors.gray(message);
      case LogLevel.INFO:
        return colors.blue(message);
      case LogLevel.WARN:
        return colors.yellow(message);
      case LogLevel.ERROR:
        return colors.red(message);
      default:
        return message;
    }
  }

  /**
   * Log a debug message
   * 
   * @param message Message to log
   * @param args Additional arguments
   */
  debug(message: string, ...args: any[]): void {
    if (shouldLog(LogLevel.DEBUG)) {
      const formattedMessage = this.formatMessage('DEBUG', message, args);
      _console.log(this.colorize(LogLevel.DEBUG, formattedMessage));
    }
  }

  /**
   * Log an info message
   * 
   * @param message Message to log
   * @param args Additional arguments
   */
  info(message: string, ...args: any[]): void {
    if (shouldLog(LogLevel.INFO)) {
      const formattedMessage = this.formatMessage('INFO', message, args);
      _console.log(this.colorize(LogLevel.INFO, formattedMessage));
    }
  }

  /**
   * Log a warning message
   * 
   * @param message Message to log
   * @param args Additional arguments
   */
  warn(message: string, ...args: any[]): void {
    if (shouldLog(LogLevel.WARN)) {
      const formattedMessage = this.formatMessage('WARN', message, args);
      _console.warn(this.colorize(LogLevel.WARN, formattedMessage));
    }
  }

  /**
   * Log an error message
   * 
   * @param message Message to log
   * @param args Additional arguments
   */
  error(message: string, ...args: any[]): void {
    if (shouldLog(LogLevel.ERROR)) {
      const formattedMessage = this.formatMessage('ERROR', message, args);
      _console.error(this.colorize(LogLevel.ERROR, formattedMessage));
    }
  }

  /**
   * Create a child logger with a more specific module name
   * 
   * @param subModule Name of the sub-module
   * @returns New logger instance
   */
  child(subModule: string): Logger {
    return new Logger(`${this.moduleName}:${subModule}`, this.config);
  }

  /**
   * Format a section header to clearly separate logical parts of execution
   * 
   * @param title Section title
   * @returns Logger instance for chaining
   */
  section(title: string): Logger {
    if (shouldLog(LogLevel.INFO)) {
      const separator = '='.repeat(Math.max(80 - title.length - 4, 10));
      const message = `${separator} ${title} ${separator}`;
      _console.log(this.colorize(LogLevel.INFO, message));
    }
    return this;
  }

  /**
   * Create a progress indicator
   * 
   * @param title Title of the operation
   * @param total Total number of items to process
   * @returns Object with update and complete methods
   */
  progress(title: string, total: number) {
    let current = 0;
    const startTime = Date.now();
    
    const update = (increment = 1, message?: string) => {
      if (!shouldLog(LogLevel.INFO)) return;
      
      current += increment;
      const percentage = Math.min(Math.floor((current / total) * 100), 100);
      const elapsed = (Date.now() - startTime) / 1000;
      let rate = current / elapsed;
      
      let timeRemaining = '';
      if (rate > 0 && current < total) {
        const remainingSecs = (total - current) / rate;
        timeRemaining = `, ETA: ${Math.floor(remainingSecs / 60)}m ${Math.floor(remainingSecs % 60)}s`;
      }
      
      const progressBar = this.createProgressBar(percentage);
      
      const statusMsg = message ? ` - ${message}` : '';
      _console.log(this.colorize(
        LogLevel.INFO, 
        this.formatMessage('INFO', `${title}: ${progressBar} ${percentage}% (${current}/${total}${timeRemaining})${statusMsg}`)
      ));
    };
    
    const complete = (message = 'Completed') => {
      if (!shouldLog(LogLevel.INFO)) return;
      
      const elapsed = (Date.now() - startTime) / 1000;
      const rate = total / elapsed;
      
      _console.log(this.colorize(
        LogLevel.INFO,
        this.formatMessage('INFO', `${title}: ${this.createProgressBar(100)} 100% (${total}/${total}) - ${message} in ${elapsed.toFixed(2)}s (${rate.toFixed(2)} items/sec)`)
      ));
    };
    
    return { update, complete };
  }

  /**
   * Create a visual progress bar
   * 
   * @param percentage Completion percentage
   * @returns Visual progress bar
   */
  private createProgressBar(percentage: number): string {
    const width = 20;
    const completeChars = Math.floor((percentage / 100) * width);
    const incompleteChars = width - completeChars;
    
    let bar = '[';
    if (this.config.useColor) {
      bar += colors.green('='.repeat(completeChars));
      bar += ' '.repeat(incompleteChars);
    } else {
      bar += '='.repeat(completeChars);
      bar += ' '.repeat(incompleteChars);
    }
    bar += ']';
    
    return bar;
  }
}
  
// Create a default logger instance
const defaultLogger = new Logger('app');

export { Logger, LogLevel, defaultLogger };