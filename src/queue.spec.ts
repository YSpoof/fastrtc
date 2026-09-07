import { describe, expect, it, vi } from "vitest";
import { createQueue } from "./queue";

describe("createQueue", () => {
  it("runs jobs one at a time", async () => {
    const enqueue = createQueue();
    const order: number[] = [];

    const first = enqueue(async () => {
      await new Promise((r) => setTimeout(r, 10));
      order.push(1);
    });
    const second = enqueue(async () => {
      order.push(2);
    });

    await Promise.all([first, second]);
    expect(order).toEqual([1, 2]);
  });

  it("continues after a job throws", async () => {
    const enqueue = createQueue();
    const order: number[] = [];

    await expect(
      enqueue(async () => {
        order.push(1);
        throw new Error("fail");
      }),
    ).rejects.toThrow("fail");

    await enqueue(async () => {
      order.push(2);
    });

    expect(order).toEqual([1, 2]);
  });

  it("preserves enqueue order under concurrent callers", async () => {
    const enqueue = createQueue();
    const order: number[] = [];

    await Promise.all([
      enqueue(async () => {
        await new Promise((r) => setTimeout(r, 5));
        order.push(1);
      }),
      enqueue(async () => {
        order.push(2);
      }),
      enqueue(async () => {
        order.push(3);
      }),
    ]);

    expect(order).toEqual([1, 2, 3]);
  });

  it("does not let a slow job block the queue forever after resolve", async () => {
    const enqueue = createQueue();
    const fn = vi.fn();

    await enqueue(async () => {
      fn("a");
    });
    await enqueue(async () => {
      fn("b");
    });

    expect(fn.mock.calls).toEqual([["a"], ["b"]]);
  });
});
