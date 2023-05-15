import AsyncLock from "async-lock"

const lock = new AsyncLock()

export async function locked<T>(key: string, cb: () => Promise<T>): Promise<T> {
    return new Promise((resolve) => {
        lock.acquire(key, async done => {
            const response = await cb();
            done()
            resolve(response)
        });
    });
}