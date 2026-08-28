# EXPLAIN.md

# What Is Khaos, and What Problem Does It Solve?

This document explains the project in plain language (no prior systems or Go experience assumed). If you already understand CPUs and concurrency, the README is a faster read. This one is for understanding *why* Khaos exists and *how* it actually works, step by step.

## The Problem, With an Everyday Example

Imagine a single chef in a kitchen who can only do one thing at a time, in the exact order the orders come in. Order #1 is a salad and it takes 30 seconds. Order #2 is a steak — takes 10 minutes because it needs to slowly cook. Order #3 is another salad — 30 seconds.

If the chef works strictly in order, order #3's customer waits over 10 minutes for a 30-second salad, purely because it happened to be queued behind a slow steak. Nothing about the salad depends on the steak. The wait is pure waste.

This is exactly what happens inside a database or any system that processes a batch of requests one at a time. If request #12 has to wait on a slow disk, request #13 which might have absolutely nothing to do with request #12 just sits there doing nothing, even though it's ready to run immediately.

Modern CPUs solved this problem years ago. They don't execute instructions strictly in the order they appear instead they look ahead, find instructions that are ready to go, and run those while the slow one is still waiting. Khaos takes that same idea and applies it to a batch of data operations (reads, writes, and deletes) instead of CPU instructions.

## The Core Idea: Reorder, But Never Break Correctness

The tricky part is that you can't just run things in any order you like. If Order #1 writes "the steak is medium-rare" and Order #2 reads "how is the steak cooked," Order #2 has to wait for Order #1, otherwise it might read stale or wrong information. This is called a **data hazard**.

Khaos's entire design is about answering one question for every single operation: *does this actually have to wait for something else, or can it run right now?* Then it runs everything that can run right now, as fast as possible, all while guaranteeing that anything with a real dependency still waits for it and guaranteeing that whoever asked for the results still gets them back in the original order they asked, even though the actual work happened out of order behind the scenes.

## The Four Pieces, Explained Simply

### 1. The Storage: A Filing Cabinet With Many Drawers

Instead of one big filing cabinet with one lock on it (meaning only one person can touch it at a time, for anything), Khaos splits storage into 256 separate drawers, each with its own lock. A piece of data's key decides which drawer it lives in. Two people can open two different drawers at the same time with zero conflict since they only have to wait for each other if they need the *same* drawer. This is what actually allows multiple operations to happen simultaneously in the first place.

### 2. The Hazard Resolver: The Kitchen's Order Board

Before anything is cooked, the resolver looks at the whole stack of orders and draws arrows between the ones that depend on each other - "this read has to happen after that write," "this write has to happen after that read finishes," and so on. The result is a map showing exactly which orders are ready to start immediately (no arrows pointing into them) and which ones have to wait for something upstream.

Importantly, the resolver builds this map in a single pass through the order list as it doesn't compare every order against every other order (which would get painfully slow as the batch grows). It just remembers "what was the last thing that touched this particular ingredient" as it walks through the list once, front to back.

### 3. The Worker Pool: Several Chefs Working the Kitchen

A handful of workers (think: several chefs, usually one per CPU core available on the machine) pick up orders that are ready to go and cook them. The moment a chef finishes an order, they check the order board: did finishing this order just make some other order ready to go? If so, that new order goes straight into the queue for any available chef to pick up next including the chef who just finished, if they're free.

This is the part that actually delivers the speedup: while one chef is stuck waiting 10 minutes on a steak, the other chefs are freely working through every other order that doesn't depend on that steak.

### 4. The Reorder Buffer: The Waiter Who Serves in the Right Order

Because orders finish whenever they finish and not necessarily in the order they were placed, someone has to make sure the customer still gets their food served in a sensible sequence, and doesn't get confused results. The Reorder Buffer holds onto finished orders and only sends one out the door once every order that was placed before it has already gone out. From the customer's point of view, everything arrives in perfect original order but they never see any of the reordering that happened in the kitchen.

## Walking Through a Real Example

Say you hand Khaos this list of operations:

1. Write "Alice's balance = 100"
2. Read "Alice's balance"
3. Write "Bob's balance = 50"

The Hazard Resolver looks at this and sees:
- Operation 2 (read Alice) depends on Operation 1 (write Alice) — same key, so it must wait.
- Operation 3 (write Bob) depends on nothing — it's a completely different key.

So Operations 1 and 3 are both immediately ready. Two chefs (worker goroutines) can grab them at the same time and start right away. Operation 2 has to wait until Operation 1 actually finishes writing.

The moment Operation 1 finishes, the worker pool notices Operation 2 just became ready, and hands it to the next available chef. Operation 3, meanwhile, might have finished first, second, or even simultaneously with Operation 1, it doesn't matter, since nothing depends on it.

Regardless of the actual finishing order, the Reorder Buffer makes sure you receive the results back in the order 1, 2, 3 — exactly as you submitted them — even though behind the scenes, 1 and 3 may have run at the same time, and 3 may have technically finished before 1 did.

## Why This Matters

The payoff is speed without sacrificing correctness. A batch full of independent operations runs close to as fast as your hardware allows, because nothing sits around waiting unless it genuinely has to. And because the hazard detection is automatic and mathematically enforced (not something the programmer has to reason about by hand for every batch), you get that speed without taking on the risk of data corruption, stale reads, or out-of-order writes — the exact bugs that make naive "just run everything in parallel" approaches dangerous.