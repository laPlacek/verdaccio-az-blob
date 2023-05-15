import { Logger } from "@verdaccio/types"

export type Log = (template: string, conf?: any) => void

export function getTrace(logger: Logger): Log {
    return (template: string, conf?: any) => logger.trace(conf || {}, template)
}

export function getError(logger: Logger): Log {
    return (template: string, conf?: any) => logger.error(conf || {}, template)
}