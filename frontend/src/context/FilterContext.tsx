"use client";

import { createContext, useContext, useState, ReactNode } from "react";

interface FilterState {
  game: string;
  dateStart: string;
  dateEnd: string;
  setGame: (g: string) => void;
  setDateStart: (d: string) => void;
  setDateEnd: (d: string) => void;
}

const FilterContext = createContext<FilterState>({
  game: "all",
  dateStart: "",
  dateEnd: "",
  setGame: () => {},
  setDateStart: () => {},
  setDateEnd: () => {},
});

export function FilterProvider({ children }: { children: ReactNode }) {
  const [game, setGame] = useState("all");
  const [dateStart, setDateStart] = useState("");
  const [dateEnd, setDateEnd] = useState("");

  return (
    <FilterContext.Provider
      value={{ game, dateStart, dateEnd, setGame, setDateStart, setDateEnd }}
    >
      {children}
    </FilterContext.Provider>
  );
}

export function useFilters() {
  return useContext(FilterContext);
}
