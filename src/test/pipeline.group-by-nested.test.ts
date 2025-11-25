import { createPipeline } from "../index";
import { createTestPipeline } from "./helpers";

describe('pipeline groupBy nested', () => {
    it('should group by nested key property', () => {
        const [pipeline, getOutput] = createTestPipeline(() => 
            createPipeline<{ state: string, city: string, town: string, population: number }>()
                .groupBy(['state', 'city'], 'towns')
                .groupBy(['state'], 'cities')
        );

        pipeline.add("town1", { state: 'TX', city: 'Dallas', town: 'Plano', population: 1000000 });
        pipeline.add("town2", { state: 'TX', city: 'Dallas', town: 'Richardson', population: 2000000 });
        pipeline.add("town3", { state: 'TX', city: 'Dallas', town: 'Carrollton', population: 3000000 });
        pipeline.add("town4", { state: 'TX', city: 'Houston', town: 'Houston', population: 5000000 });
        pipeline.add("town5", { state: 'TX', city: 'Houston', town: 'Katy', population: 6000000 });
        pipeline.add("town6", { state: 'TX', city: 'Houston', town: 'Sugar Land', population: 7000000 });
        pipeline.add("town7", { state: 'OK', city: 'Oklahoma City', town: 'Oklahoma City', population: 9000000 });
        pipeline.add("town8", { state: 'OK', city: 'Oklahoma City', town: 'Edmond', population: 10000000 });
        pipeline.add("town9", { state: 'OK', city: 'Tulsa', town: 'Tulsa', population: 10000000 });
        pipeline.add("town10", { state: 'OK', city: 'Tulsa', town: 'Broken Arrow', population: 11000000 });
        pipeline.add("town11", { state: 'OK', city: 'Tulsa', town: 'Jenks', population: 13000000 });

        const output = getOutput();
        expect(output.length).toBe(2);
        expect(output[0].state).toBe('TX');
        expect(output[0].cities).toEqual([
            {
                city: 'Dallas',
                towns: [
                    { town: 'Plano', population: 1000000 },
                    { town: 'Richardson', population: 2000000 },
                    { town: 'Carrollton', population: 3000000 }
                ]
            },
            {
                city: 'Houston',
                towns: [
                    { town: 'Houston', population: 5000000 },
                    { town: 'Katy', population: 6000000 },
                    { town: 'Sugar Land', population: 7000000 }
                ]
            },
        ]);
        expect(output[1].state).toBe('OK');
        expect(output[1].cities).toEqual([
            {
                city: 'Oklahoma City',
                towns: [
                    { town: 'Oklahoma City', population: 9000000 },
                    { town: 'Edmond', population: 10000000 }
                ]
            },
            {
                city: 'Tulsa',
                towns: [
                    { town: 'Tulsa', population: 10000000 },
                    { town: 'Broken Arrow', population: 11000000 },
                    { town: 'Jenks', population: 13000000 }
                ]
            }
        ]);
    });
});