import sys
import numpy as np
import pandas as pd
from tkinter import messagebox
import datetime
import tkinter as tk

# defenition for input variables

def input(txt1, txt2, btn, title):
    master = tk.Tk()
    master.wm_title(title)

    tk.Label(master, 
         text=txt1).grid(row=0)
    tk.Label(master, 
         text=txt2).grid(row=1)

    txt1 = tk.Entry(master)
    txt2 = tk.Entry(master)

    txt1.grid(row=0, column=1)
    txt2.grid(row=1, column=1)

    tk.Button(master, 
          text=btn, 
          command=master.quit).grid(row=3, 
                                    column=0, 
                                    sticky=tk.W, 
                                    pady=4)
    tk.mainloop()
    txt1 = txt1.get()
    txt2 = txt2.get()
    return txt1, txt2