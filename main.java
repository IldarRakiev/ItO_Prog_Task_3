import org.w3c.dom.ls.LSOutput;

import static java.util.Arrays.stream;

import java.util.Arrays;
import java.util.concurrent.*;
import java.util.ArrayList;

public class Main {
    public static void main(String[] args) throws Exception {
        tester();
    }

    public static void tester() throws Exception {
        int[] supply1 = {30, 40, 50};
        int[] demand1 = {20, 30, 40, 30};
        int[][] cost1 = {{8, 6, 10, 9}, {9, 12, 3, 7}, {4, 14, 5, 8}};

        int[] supply2 = {20, 30, 25};
        int[] demand2 = {15, 25, 20, 15};
        int[][] cost2 = {{4, 3, 2, 1}, {2, 4, 3, 5}, {3, 1, 4, 2}};

        int[] supply3 = {20, 30, 25};
        int[] demand3 = {10, 25, 15, 25};
        int[][] cost3 = {{8, 6, 10, 9}, {9, 12, 13, 7}, {14, 9, 16, 5}};

        int[] supply4 = {10, 15, 6};
        int[] demand4 = {2, 25, 18, 6};
        int[][] cost4 = {{1, 2, 2, 3}, {1, 4, 3, 1}, {3, 7, 8, 4}};

        int[] supply5 = {3, 48, 22};
        int[] demand5 = {2, 23, 25, 23};
        int[][] cost5 = {{3, 3, 5, 7}, {1, 1, -2, 3}, {4, 1, 2, 3}};

        ArrayList<Problem> problems = new ArrayList<>();

        Problem problem1 = new Problem(supply1, demand1, cost1);
        Problem problem2 = new Problem(supply2, demand2, cost2);
        Problem problem3 = new Problem(supply3, demand3, cost3);
        Problem problem4 = new Problem(supply4, demand4, cost4);
        Problem problem5 = new Problem(supply5, demand5, cost5);


        problems.add(problem1);
        problems.add(problem2);
        problems.add(problem3);
        problems.add(problem4);
        problems.add(problem5);


        ExecutorService es = Executors.newFixedThreadPool(2);

        int test = 1;
        for (Problem problem : problems) {
            System.out.println("Test №" + test + ":");
            System.out.println();
            test++;

            int[] supply = problem.getSupply();
            int[] demand = problem.getDemand();
            int[][] cost = problem.getCost();

            printGivenData(problem);

            if (Arrays.stream(supply).sum() != Arrays.stream(demand).sum()) {
                System.out.println("The problem is not balanced!");
            } else {
                if (!isMethodApplicable(problem)) {
                    System.out.println("The method is not applicable!");
                } else {
                    int[][] nwcSolution = NorthWestCornerMethod.solve(problem);
                    System.out.println("North-West Corner Solution:");
                    printSolution(nwcSolution);

                    System.out.println("Vogel’s Approximation Solution:");
                    int[][] vamSolution = VogelsApproximationMethod.solve(problem, es);
                    printSolution(vamSolution);

                    System.out.println("Russell’s Approximation Solution:");
                    int[][] ramSolution = RussellsApproximationMethod.solve(problem);
                    printSolution(ramSolution);
                }
            }

            System.out.println("---------------------------");
            System.out.println();
        }

        es.shutdown();
        es.awaitTermination(1, TimeUnit.MINUTES);
    }

    public static void printGivenData(Problem problem) {
        int[] supply = problem.getSupply();
        int[] demand = problem.getDemand();
        int[][] cost = problem.getCost();

        System.out.println("Given data");
        System.out.print("    ");
        System.out.println("D1   D2   D3   D4   Supply");
        for (int i = 0; i < cost.length; i++) {
            System.out.print("S" + (i + 1) + "  ");
            for (int j = 0; j < cost[i].length; j++) {
                System.out.print(cost[i][j] + "    ");
            }
            System.out.println(supply[i]);
        }
        System.out.print("    ");
        for (int i = 0; i < demand.length; i++) {
            System.out.print(demand[i] + "   ");
        }
        System.out.println();
        System.out.println();
    }

    private static boolean isMethodApplicable(Problem problem) {
        for (int[] row : problem.getCost()) {
            for (int cost : row) {
                if (cost < 0) {
                    return false;
                }
            }
        }
        return true;
    }

    private static void printSolution(int[][] solution) {
        for (int[] ints : solution) {
            for (int anInt : ints) {
                System.out.print(anInt + "\t");
            }
            System.out.println();
        }
    }
}

class Problem {
    private final int[] supply;
    private final int[] demand;
    private final int[][] cost;

    public Problem(int[] supply, int[] demand, int[][] cost) {
        this.supply = supply;
        this.demand = demand;
        this.cost = cost;
    }

    public int[] getSupply() {
        return supply;
    }

    public int[] getDemand() {
        return demand;
    }

    public int[][] getCost() {
        return cost;
    }
}

class NorthWestCornerMethod {
    public static int[][] solve(Problem problem) {
        int[] supply = problem.getSupply().clone();
        int[] demand = problem.getDemand().clone();
        int[][] allocation = new int[supply.length][demand.length];

        int i = 0, j = 0;
        while (i < supply.length && j < demand.length) {
            int min = Math.min(supply[i], demand[j]);
            allocation[i][j] = min;
            supply[i] -= min;
            demand[j] -= min;
            if (supply[i] == 0) {
                i++;
            } else {
                j++;
            }
        }
        return allocation;
    }
}

class VogelsApproximationMethod {

    static boolean[] rowDone;
    static boolean[] colDone;
    static int[][] result;

    public static int[][] solve(Problem problem, ExecutorService es) throws Exception {
        int[] supply = problem.getSupply().clone();
        int[] demand = problem.getDemand().clone();
        int[][] costs = problem.getCost();

        int nRows = supply.length;
        int nCols = demand.length;

        rowDone = new boolean[nRows];
        colDone = new boolean[nCols];
        result = new int[nRows][nCols];

        int supplyLeft = stream(supply).sum();

        while (supplyLeft > 0) {
            int[] cell = nextCell(nRows, nCols, costs, es);
            int r = cell[0];
            int c = cell[1];

            int quantity = Math.min(demand[c], supply[r]);
            demand[c] -= quantity;
            if (demand[c] == 0) colDone[c] = true;

            supply[r] -= quantity;
            if (supply[r] == 0) rowDone[r] = true;

            result[r][c] = quantity;
            supplyLeft -= quantity;
        }

        return result;
    }

    static int[] nextCell(int nRows, int nCols, int[][] costs, ExecutorService es) throws Exception {
        Future<int[]> f1 = es.submit(() -> maxPenalty(nRows, nCols, true, costs));
        Future<int[]> f2 = es.submit(() -> maxPenalty(nCols, nRows, false, costs));

        int[] res1 = f1.get();
        int[] res2 = f2.get();

        if (res1[3] == res2[3]) return res1[2] < res2[2] ? res1 : res2;

        return (res1[3] > res2[3]) ? res2 : res1;
    }

    static int[] diff(int j, int len, boolean isRow, int[][] costs) {
        int min1 = Integer.MAX_VALUE, min2 = Integer.MAX_VALUE;
        int minP = -1;
        for (int i = 0; i < len; i++) {
            if (isRow ? colDone[i] : rowDone[i]) continue;
            int c = isRow ? costs[j][i] : costs[i][j];
            if (c < min1) {
                min2 = min1;
                min1 = c;
                minP = i;
            } else if (c < min2) min2 = c;
        }
        return new int[]{min2 - min1, min1, minP};
    }

    static int[] maxPenalty(int len1, int len2, boolean isRow, int[][] costs) {
        int md = Integer.MIN_VALUE;
        int pc = -1, pm = -1, mc = -1;
        for (int i = 0; i < len1; i++) {
            if (isRow ? rowDone[i] : colDone[i]) continue;
            int[] res = diff(i, len2, isRow, costs);
            if (res[0] > md) {
                md = res[0];
                pm = i;
                mc = res[1];
                pc = res[2];
            }
        }
        return isRow ? new int[]{pm, pc, mc, md} : new int[]{pc, pm, mc, md};
    }
}

class RussellsApproximationMethod {

    static boolean[] rowDone;
    static boolean[] colDone;
    static int[][] result;

    public static int[][] solve(Problem problem) {
        int[] supply = problem.getSupply().clone();
        int[] demand = problem.getDemand().clone();
        int[][] costs = problem.getCost();

        int nRows = supply.length;
        int nCols = demand.length;

        rowDone = new boolean[nRows];
        colDone = new boolean[nCols];
        result = new int[nRows][nCols];

        int supplyLeft = stream(supply).sum();

        while (supplyLeft > 0) {
            int[] cell = nextCell(nRows, nCols, costs);
            int r = cell[0];
            int c = cell[1];

            int quantity = Math.min(demand[c], supply[r]);
            demand[c] -= quantity;
            if (demand[c] == 0) colDone[c] = true;

            supply[r] -= quantity;
            if (supply[r] == 0) rowDone[r] = true;

            result[r][c] = quantity;
            supplyLeft -= quantity;
        }

        return result;
    }

    static int[] nextCell(int nRows, int nCols, int[][] costs) {
        double[][] u = new double[nRows][nCols];
        double[][] v = new double[nRows][nCols];

        for (int i = 0; i < nRows; i++) {
            for (int j = 0; j < nCols; j++) {
                if (!rowDone[i] && !colDone[j]) {
                    u[i][j] = v[i][j] = 1.0 / (costs[i][j] + 1.0);
                }
            }
        }

        int[] maxCell = new int[2];
        double maxRatio = -1;

        for (int i = 0; i < nRows; i++) {
            for (int j = 0; j < nCols; j++) {
                if (rowDone[i] || colDone[j]) continue;

                double sumU = 0, sumV = 0;
                for (int k = 0; k < nCols; k++) sumU += u[i][k];
                for (int k = 0; k < nRows; k++) sumV += v[k][j];

                double ratio = (u[i][j] / sumU) + (v[i][j] / sumV);

                if (ratio > maxRatio) {
                    maxRatio = ratio;
                    maxCell[0] = i;
                    maxCell[1] = j;
                }
            }
        }

        return maxCell;
    }
}
