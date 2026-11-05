# Agent Rules

1. Java 21, Maven, JUnit 5. No Spring or other frameworks inside nexus-core.
2. Before coding: explain the concept and design in under 10 lines and list the files you will create.
3. Implement only the current task. No fake functionality, no hardcoded benchmark numbers.
4. Every component gets tests. Run mvn -q verify.
5. Inspect existing code first. Do not rewrite working code.
6. Comment non-obvious concurrency and crash-safety invariants.
7. Update docs/ and the README Known Limitations when behavior changes.
8. Finish routine: tests green -> git add -A -> commit with the message given in the task -> git push origin main. Never push failing tests.
9. End every task with: what works, what is untested, known limitations, and 3 interview questions about the component.

