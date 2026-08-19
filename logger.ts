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
    level: LogLevel;
    useTimestamp: boolean;
    useColor: boolean;
    logToFile?: string;
    prettyPrint?: boolean;
  }
  
  /**
   * When DOC2VEC_STRUCTURED_LOGS=1 (set by the controller when spawning sync jobs),
   * every log line is emitted as a single JSON object so a parent process can parse
   * the stream reliably. Human-formatted output is suppressed in this mode.
   */
  const STRUCTURED = process.env.DOC2VEC_STRUCTURED_LOGS === '1';

  function emitStructured(payload: Record<string, any>, useStderr = false): void {
    const line = JSON.stringify(payload);
    if (useStderr) {
      process.stderr.write(line + '\n');
    } else {
      process.stdout.write(line + '\n');
    }
  }

  function stringifyArg(arg: any): string {
    if (arg instanceof Error) {
      return `${arg.message}${arg.stack ? `\n${arg.stack}` : ''}`;
    }
    if (typeof arg === 'object' && arg !== null) {
      try {
        return JSON.stringify(arg);
      } catch (e) {
        return '[Unserializable Object]';
      }
    }
    return String(arg);
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
        level: LogLevel.INFO,
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
    private emitJson(level: string, message: string, args: any[], useStderr = false): void {
      const msg = args.length > 0
        ? `${message} ${args.map(stringifyArg).join(' ')}`
        : message;
      emitStructured({
        ts: new Date().toISOString(),
        level,
        module: this.moduleName,
        msg
      }, useStderr);
    }

    debug(message: string, ...args: any[]): void {
      if (this.config.level <= LogLevel.DEBUG) {
        if (STRUCTURED) {
          this.emitJson('debug', message, args);
          return;
        }
        const formattedMessage = this.formatMessage('DEBUG', message, args);
        console.log(this.colorize(LogLevel.DEBUG, formattedMessage));
      }
    }
  
    /**
     * Log an info message
     * 
     * @param message Message to log
     * @param args Additional arguments
     */
    info(message: string, ...args: any[]): void {
      if (this.config.level <= LogLevel.INFO) {
        if (STRUCTURED) {
          this.emitJson('info', message, args);
          return;
        }
        const formattedMessage = this.formatMessage('INFO', message, args);
        console.log(this.colorize(LogLevel.INFO, formattedMessage));
      }
    }
  
    /**
     * Log a warning message
     * 
     * @param message Message to log
     * @param args Additional arguments
     */
    warn(message: string, ...args: any[]): void {
      if (this.config.level <= LogLevel.WARN) {
        if (STRUCTURED) {
          this.emitJson('warn', message, args, true);
          return;
        }
        const formattedMessage = this.formatMessage('WARN', message, args);
        console.warn(this.colorize(LogLevel.WARN, formattedMessage));
      }
    }
  
    /**
     * Log an error message
     * 
     * @param message Message to log
     * @param args Additional arguments
     */
    error(message: string, ...args: any[]): void {
      if (this.config.level <= LogLevel.ERROR) {
        if (STRUCTURED) {
          this.emitJson('error', message, args, true);
          return;
        }
        const formattedMessage = this.formatMessage('ERROR', message, args);
        console.error(this.colorize(LogLevel.ERROR, formattedMessage));
      }
    }

    /**
     * Emit a structured event (e.g. run-summary). In structured mode this prints a
     * machine-parseable JSON line; otherwise it logs the payload at info level.
     *
     * @param name Event name
     * @param data Event payload
     */
    event(name: string, data: Record<string, any> = {}): void {
      if (STRUCTURED) {
        emitStructured({ event: name, ts: new Date().toISOString(), ...data });
        return;
      }
      this.info(`[event:${name}]`, data);
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
      if (this.config.level <= LogLevel.INFO) {
        if (STRUCTURED) {
          emitStructured({ event: 'section', ts: new Date().toISOString(), module: this.moduleName, title });
          return this;
        }
        const separator = '='.repeat(Math.max(80 - title.length - 4, 10));
        const message = `${separator} ${title} ${separator}`;
        console.log(this.colorize(LogLevel.INFO, message));
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
      // In structured mode, throttle progress lines to at most one per second so a
      // large crawl doesn't flood the parent process / database with log rows.
      let lastStructuredEmit = 0;

      const update = (increment = 1, message?: string) => {
        if (this.config.level > LogLevel.INFO) return;

        current += increment;

        if (STRUCTURED) {
          const now = Date.now();
          if (now - lastStructuredEmit >= 1000 || current === total) {
            lastStructuredEmit = now;
            emitStructured({ event: 'progress', ts: new Date().toISOString(), module: this.moduleName, title, current, total, message });
          }
          return;
        }
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
        console.log(this.colorize(
          LogLevel.INFO, 
          this.formatMessage('INFO', `${title}: ${progressBar} ${percentage}% (${current}/${total}${timeRemaining})${statusMsg}`)
        ));
      };
      
      const complete = (message = 'Completed') => {
        if (this.config.level > LogLevel.INFO) return;

        const elapsed = (Date.now() - startTime) / 1000;
        const rate = total / elapsed;

        if (STRUCTURED) {
          emitStructured({ event: 'progress', ts: new Date().toISOString(), module: this.moduleName, title, current: total, total, message: `${message} in ${elapsed.toFixed(2)}s`, done: true });
          return;
        }
        
        console.log(this.colorize(
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