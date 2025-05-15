#!/bin/bash

gdb -tui -p "$(pidof observer)"
