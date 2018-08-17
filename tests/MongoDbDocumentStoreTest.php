<?php
/**
 * This file is part of the proophsoftware/mongodb-document-store.
 * (c) 2018 prooph software GmbH <contact@prooph.de>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventMachineTest\MongoDb;

use MongoDB\Client;
use PHPUnit\Framework\TestCase;
use Prooph\EventMachine\MongoDb\Exception\RuntimeException;
use Prooph\EventMachine\MongoDb\MongoDbDocumentStore;
use Prooph\EventMachine\Persistence\DocumentStore\FieldIndex;
use Prooph\EventMachine\Persistence\DocumentStore\Filter;
use Prooph\EventMachine\Persistence\DocumentStore\OrderBy;
use Prooph\EventMachine\Persistence\DocumentStore\Index;
use Prooph\EventMachine\Persistence\DocumentStore\MultiFieldIndex;
use Ramsey\Uuid\Uuid;

class MongoDbDocumentStoreTest extends TestCase
{
    private CONST TABLE_PREFIX = 'test_';

    /**
     * @var MongoDbDocumentStore
     */
    protected $documentStore;

    /**
     * @var Client
     */
    private $client;

    protected function setUp()
    {
        $this->client = TestUtil::getConnection();

        $this->documentStore = new MongoDbDocumentStore($this->client, TestUtil::getDatabaseName(), self::TABLE_PREFIX);
    }

    public function tearDown()
    {
        TestUtil::tearDownDatabase();
    }

    /**
     * @test
     */
    public function it_adds_collection(): void
    {
        $this->documentStore->addCollection('mycol');
        $this->assertTrue($this->documentStore->hasCollection('mycol'));
    }

    /**
     * @test
     */
    public function it_adds_collection_with_field_index_asc(): void
    {
        $collectionName = 'test_field_index_asc';
        $this->documentStore->addCollection($collectionName, FieldIndex::forField('field_asc'));
        $this->assertTrue($this->documentStore->hasCollection($collectionName));

        $indexes = $this->getIndexes($collectionName);

        $this->assertCount(2, $indexes);
        $this->assertSame(1, $indexes[1]['key']['field_asc']);
    }

    /**
     * @test
     */
    public function it_adds_collection_with_field_index_desc(): void
    {
        $collectionName = 'test_field_index_desc';
        $this->documentStore->addCollection($collectionName, FieldIndex::forField('field_desc', Index::SORT_DESC));
        $this->assertTrue($this->documentStore->hasCollection($collectionName));

        $indexes = $this->getIndexes($collectionName);

        $this->assertCount(2, $indexes);
        $this->assertSame(-1, $indexes[1]['key']['field_desc']);
    }

    /**
     * @test
     */
    public function it_adds_collection_with_field_index_unique(): void
    {
        $collectionName = 'test_field_index_unique';
        $this->documentStore->addCollection($collectionName, FieldIndex::forField('field_asc', Index::SORT_DESC, true));
        $this->assertTrue($this->documentStore->hasCollection($collectionName));

        $indexes = $this->getIndexes($collectionName);

        $this->assertCount(2, $indexes);
        $this->assertSame(-1, $indexes[1]['key']['field_asc']);
        $this->assertTrue($indexes[1]['unique']);
    }

    /**
     * @test
     */
    public function it_adds_collection_with_multi_field_index_asc(): void
    {
        $collectionName = 'test_multi_field_index_asc';
        $this->documentStore->addCollection($collectionName, MultiFieldIndex::forFields(['a', 'b']));
        $this->assertTrue($this->documentStore->hasCollection($collectionName));

        $indexes = $this->getIndexes($collectionName);

        $this->assertCount(2, $indexes);

        $this->assertSame(1, $indexes[1]['key']['a']);
        $this->assertSame(1, $indexes[1]['key']['b']);
    }

    /**
     * @test
     */
    public function it_adds_collection_with_multi_field_index_unique(): void
    {
        $collectionName = 'test_multi_field_index_unique';
        $this->documentStore->addCollection($collectionName, MultiFieldIndex::forFields(['a', 'b'], true));
        $this->assertTrue($this->documentStore->hasCollection($collectionName));

        $indexes = $this->getIndexes($collectionName);

        $this->assertCount(2, $indexes);

        $this->assertSame(1, $indexes[1]['key']['a']);
        $this->assertSame(1, $indexes[1]['key']['b']);

        $this->assertTrue($indexes[1]['unique']);
    }

    /**
     * @test
     */
    public function it_list_collections(): void
    {
        $this->client->selectDatabase(TestUtil::getDatabaseName())->createCollection('unkown');
        $this->documentStore->addCollection('one');
        $this->documentStore->addCollection('two');

        $collections = $this->documentStore->listCollections();
        $this->assertCount(2, $collections);
        $this->assertTrue(in_array('one', $collections, true));
        $this->assertTrue(in_array('two', $collections, true));
    }

    /**
     * @test
     */
    public function it_filters_collections_by_prefix(): void
    {
        $this->client->selectDatabase(TestUtil::getDatabaseName())->createCollection('unkown');
        $this->documentStore->addCollection('my_one');
        $this->documentStore->addCollection('my_two');

        $collections = $this->documentStore->filterCollectionsByPrefix('my_');
        $this->assertCount(2, $collections);
        $this->assertTrue(in_array('my_one', $collections, true));
        $this->assertTrue(in_array('my_two', $collections, true));
    }

    /**
     * @test
     */
    public function it_adds_and_reads__document(): void
    {
        $id = Uuid::uuid4()->toString();
        $animal = [
            'animal' => 'dog',
            'name' => 'Jack',
            'age' => 5,
            'character' => [
                'friendly' => 10,
                'wild' => 6,
                'docile' => 8,
            ],
        ];

        $this->documentStore->addCollection('animals', FieldIndex::forField('name', Index::SORT_ASC, true));

        $this->documentStore->addDoc('animals', $id, $animal);

        $jack = $this->documentStore->getDoc('animals', $id);

        $this->assertEquals($animal, $jack);
    }

    /**
     * @test
     */
    public function it_deletes_document(): void
    {
        $id = Uuid::uuid4()->toString();
        $animal = [
            'animal' => 'dog',
            'name' => 'Jack',
            'age' => 5,
            'character' => [
                'friendly' => 10,
                'wild' => 6,
                'docile' => 8,
            ],
        ];

        $this->documentStore->addCollection('animals', FieldIndex::forField('name', Index::SORT_ASC, true));
        $this->documentStore->addDoc('animals', $id, $animal);

        $this->documentStore->deleteDoc('animals', $id);
        $jack = $this->documentStore->getDoc('animals', $id);

        $this->assertNull($jack);
    }

    /**
     * @test
     */
    public function it_upserts_document(): void
    {
        $id = Uuid::uuid4()->toString();
        $animal = [
            'animal' => 'dog',
            'name' => 'Jack',
            'age' => 5,
            'character' => [
                'friendly' => 10,
                'wild' => 6,
                'docile' => 8,
            ],
        ];

        $this->documentStore->addCollection('animals', FieldIndex::forField('name', Index::SORT_ASC, true));

        $this->documentStore->upsertDoc('animals', $id, $animal);

        $jack = $this->documentStore->getDoc('animals', $id);

        $this->assertEquals($animal, $jack);
    }

    /**
     * @test
     */
    public function it_upserts_only_part_of_document(): void
    {
        $id = Uuid::uuid4()->toString();
        $animal = [
            'animal' => 'dog',
            'name' => 'Jack',
            'age' => 5,
            'character' => [
                'friendly' => 10,
                'wild' => 6,
                'docile' => 8,
            ],
        ];

        $this->documentStore->addCollection('animals', FieldIndex::forField('name', Index::SORT_ASC, true));

        $this->documentStore->addDoc('animals', $id, $animal);
        $jack = $this->documentStore->getDoc('animals', $id);

        $this->assertEquals($animal, $jack);

        $this->documentStore->upsertDoc('animals', $id, ['friends' => 'none']);
        $jack = $this->documentStore->getDoc('animals', $id);
        $this->assertSame('none', $jack['friends']);

        unset($jack['friends']);
        $this->assertEquals($animal, $jack);
    }

    /**
     * @test
     */
    public function it_updates_only_part_of_document(): void
    {
        $id = Uuid::uuid4()->toString();
        $animal = [
            'animal' => 'dog',
            'name' => 'Jack',
            'age' => 5,
            'character' => [
                'friendly' => 10,
                'wild' => 6,
                'docile' => 8,
            ],
        ];

        $this->documentStore->addCollection('animals', FieldIndex::forField('name', Index::SORT_ASC, true));

        $this->documentStore->addDoc('animals', $id, $animal);
        $jack = $this->documentStore->getDoc('animals', $id);

        $this->assertEquals($animal, $jack);

        $this->documentStore->upsertDoc('animals', $id, ['age' => 10]);
        $jack = $this->documentStore->getDoc('animals', $id);
        $this->assertSame(10, $jack['age']);
    }

    /**
     * @test
     */
    public function it_updates_only_sub_part_of_document(): void
    {
        $id = Uuid::uuid4()->toString();
        $animal = [
            'animal' => 'dog',
            'name' => 'Jack',
            'age' => 5,
            'character' => [
                'friendly' => 10,
                'wild' => 6,
                'docile' => 8,
            ],
        ];

        $this->documentStore->addCollection('animals', FieldIndex::forField('name', Index::SORT_ASC, true));

        $this->documentStore->addDoc('animals', $id, $animal);
        $jack = $this->documentStore->getDoc('animals', $id);

        $this->assertEquals($animal, $jack);

        $this->documentStore->upsertDoc('animals', $id, ['character' => ['friendly' => 1]]);
        $jack = $this->documentStore->getDoc('animals', $id);
        $this->assertSame(1, $jack['character']['friendly']);

        $jack['character']['friendly'] = 10;
        $this->assertEquals($animal, $jack);
    }

    /**
     * @test
     */
    public function it_updates_many_documents(): void
    {
        $animal = [
            'animal' => 'dog',
            'name' => 'Jack',
            'age' => 5,
            'character' => [
                'friendly' => 5,
                'wild' => 6,
                'docile' => 8,
            ],
        ];
        $animal2 = $animal;
        $animal2['name'] = 'Neo';
        $animal3 = $animal;
        $animal3['name'] = 'Smith';

        $this->documentStore->addCollection('animals', FieldIndex::forField('name', Index::SORT_ASC, true));

        $this->documentStore->addDoc('animals', Uuid::uuid4()->toString(), $animal);
        $this->documentStore->addDoc('animals', Uuid::uuid4()->toString(), $animal2);
        $this->documentStore->addDoc('animals', Uuid::uuid4()->toString(), $animal3);

        $this->documentStore->updateMany('animals', new Filter\GteFilter('character.friendly', 5), ['pet' => true]);

        $cursor = $this->documentStore->filterDocs('animals', new Filter\EqFilter('pet', true));
        $counter = 0;

        foreach ($cursor as $item) {
            unset($item['pet']);
            switch ($item['name']) {
                case 'Jack':
                    $this->assertEquals($animal, $item);
                    break;
                case 'Neo':
                    $this->assertEquals($animal2, $item);
                    break;
                case 'Smith':
                    $this->assertEquals($animal3, $item);
                    break;
                default:
                    throw new \RuntimeException('Test failed');
                    break;
            }
            $counter++;
        }
        $this->assertEquals(3, $counter);
    }

    /**
     * @test
     */
    public function it_deletes_many_documents(): void
    {
        $animal = [
            'animal' => 'dog',
            'name' => 'Jack',
            'age' => 5,
            'character' => [
                'friendly' => 5,
                'wild' => 6,
                'docile' => 8,
            ],
        ];
        $animal2 = $animal;
        $animal2['name'] = 'Neo';
        $animal3 = $animal;
        $animal3['name'] = 'Smith';

        $this->documentStore->addCollection('animals', FieldIndex::forField('name', Index::SORT_ASC, true));

        $this->documentStore->addDoc('animals', Uuid::uuid4()->toString(), $animal);
        $this->documentStore->addDoc('animals', Uuid::uuid4()->toString(), $animal2);
        $this->documentStore->addDoc('animals', Uuid::uuid4()->toString(), $animal3);

        $this->documentStore->deleteMany('animals', new Filter\EqFilter('character.friendly', 5));

        $cursor = $this->documentStore->filterDocs('animals', new Filter\AnyFilter());
        $counter = 0;

        foreach ($cursor as $item) {
            $counter++;
        }
        $this->assertEquals(0, $counter);
    }

    /**
     * @test
     */
    public function it_orders_documents(): void
    {
        $animal = [
            'animal' => 'dog',
            'name' => 'Jack',
            'age' => 5,
            'character' => [
                'friendly' => 5,
                'wild' => 6,
                'docile' => 8,
            ],
        ];
        $animal2 = $animal;
        $animal2['name'] = 'Neo';
        $animal3 = $animal;
        $animal3['name'] = 'Smith';

        $this->documentStore->addCollection('animals', FieldIndex::forField('name', Index::SORT_ASC, true));

        $this->documentStore->addDoc('animals', Uuid::uuid4()->toString(), $animal3);
        $this->documentStore->addDoc('animals', Uuid::uuid4()->toString(), $animal);
        $this->documentStore->addDoc('animals', Uuid::uuid4()->toString(), $animal2);

        $cursor = $this->documentStore->filterDocs(
            'animals',
            new Filter\AnyFilter(),
            null,
            null,
            OrderBy\Asc::byProp('name')
        );
        $counter = 0;

        foreach ($cursor as $item) {
            switch ($counter) {
                case 0:
                    $this->assertEquals($animal, $item);
                    break;
                case 1:
                    $this->assertEquals($animal2, $item);
                    break;
                case 2:
                    $this->assertEquals($animal3, $item);
                    break;
                default:
                    throw new \RuntimeException('Test failed');
                    break;
            }
            $counter++;
        }
        $this->assertEquals(3, $counter);
    }

    /**
     * @test
     */
    public function it_filters_document_by_equal(): void
    {
        $id = Uuid::uuid4()->toString();
        $animal = [
            'animal' => 'dog',
            'name' => 'Jack',
            'age' => 5,
            'character' => [
                'friendly' => 10,
                'wild' => 6,
                'docile' => 8,
            ],
        ];

        $this->documentStore->addCollection('animals', FieldIndex::forField('name', Index::SORT_ASC, true));

        $this->documentStore->addDoc('animals', $id, $animal);

        $cursor = $this->documentStore->filterDocs('animals', new Filter\EqFilter('animal', 'dog'));

        foreach ($cursor as $jack) {
            $this->assertEquals($animal, $jack);
        }
    }

    /**
     * @test
     */
    public function it_filters_document_by_and(): void
    {
        $id = Uuid::uuid4()->toString();
        $animal = [
            'animal' => 'dog',
            'name' => 'Jack',
            'age' => 5,
            'character' => [
                'friendly' => 10,
                'wild' => 6,
                'docile' => 8,
            ],
            'color' => ['black', 'white'],
        ];

        $this->documentStore->addCollection('animals', FieldIndex::forField('name', Index::SORT_ASC, true));

        $this->documentStore->addDoc('animals', $id, $animal);

        $cursor = $this->documentStore->filterDocs(
            'animals',
            new Filter\AndFilter(
                new Filter\EqFilter('name', 'Jack'),
                new Filter\InArrayFilter('color', 'black')
            )
        );

        foreach ($cursor as $jack) {
            $this->assertEquals($animal, $jack);
        }
    }

    /**
     * @test
     */
    public function it_filters_documents_by_not(): void
    {
        $animal = [
            'animal' => 'dog',
            'name' => 'Jack',
            'age' => 5,
            'character' => [
                'friendly' => 5,
                'wild' => 6,
                'docile' => 8,
            ],
        ];
        $animal2 = $animal;
        $animal2['name'] = 'Neo';
        $animal3 = $animal;
        $animal3['name'] = 'Smith';

        $this->documentStore->addCollection('animals', FieldIndex::forField('name', Index::SORT_ASC, true));

        $this->documentStore->addDoc('animals', Uuid::uuid4()->toString(), $animal);
        $this->documentStore->addDoc('animals', Uuid::uuid4()->toString(), $animal2);
        $this->documentStore->addDoc('animals', Uuid::uuid4()->toString(), $animal3);

        $cursor = $this->documentStore->filterDocs('animals', new Filter\NotFilter(new Filter\EqFilter('name', 'Jack')));
        $counter = 0;

        foreach ($cursor as $item) {
            switch ($item['name']) {
                case 'Neo':
                    $this->assertEquals($animal2, $item);
                    break;
                case 'Smith':
                    $this->assertEquals($animal3, $item);
                    break;
                default:
                    throw new \RuntimeException('Test failed');
                    break;
            }
            $counter++;
        }
        $this->assertEquals(2, $counter);
    }

    /**
     * @test
     */
    public function it_filters_documents_by_like(): void
    {
        $animal = [
            'animal' => 'dog',
            'name' => 'Jack',
            'age' => 5,
            'character' => [
                'friendly' => 5,
                'wild' => 6,
                'docile' => 8,
            ],
        ];
        $animal2 = $animal;
        $animal2['name'] = 'John';
        $animal3 = $animal;
        $animal3['name'] = 'Smith';

        $this->documentStore->addCollection('animals', FieldIndex::forField('name', Index::SORT_ASC, true));

        $this->documentStore->addDoc('animals', Uuid::uuid4()->toString(), $animal);
        $this->documentStore->addDoc('animals', Uuid::uuid4()->toString(), $animal2);
        $this->documentStore->addDoc('animals', Uuid::uuid4()->toString(), $animal3);

        $cursor = $this->documentStore->filterDocs('animals', new Filter\LikeFilter('name', 'J'));
        $counter = 0;

        foreach ($cursor as $item) {
            switch ($item['name']) {
                case 'Jack':
                    $this->assertEquals($animal, $item);
                    break;
                case 'John':
                    $this->assertEquals($animal2, $item);
                    break;
                default:
                    throw new \RuntimeException('Test failed');
                    break;
            }
            $counter++;
        }
        $this->assertEquals(2, $counter);
    }

    /**
     * @test
     */
    public function it_can_not_add_same_doc_twice(): void
    {
        $id = Uuid::uuid4()->toString();
        $animal = [
            'animal' => 'dog',
            'name' => 'Jack',
            'age' => 5,
            'character' => [
                'friendly' => 10,
                'wild' => 6,
                'docile' => 8,
            ],
        ];

        $this->documentStore->addCollection('animals', FieldIndex::forField('name', Index::SORT_ASC, true));

        $this->documentStore->addDoc('animals', $id, $animal);

        $this->expectException(RuntimeException::class);
        $this->documentStore->addDoc('animals', Uuid::uuid4()->toString(), $animal);
    }

    private function getIndexes(string $collectionName): array
    {
        $cursor = $this->client->selectDatabase(TestUtil::getDatabaseName())
            ->selectCollection(self::TABLE_PREFIX . $collectionName)
            ->listIndexes();

        return iterator_to_array($cursor);
    }

}
