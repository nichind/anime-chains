from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from asyncio import run
from json import dump

engines = {"main": create_async_engine("sqlite+aiosqlite:///chains.sqlite")}
sessions = {
    k: sessionmaker(v, expire_on_commit=False, class_=AsyncSession)
    for k, v in engines.items()
}
Base = declarative_base()


class AnimeChains(Base):
    __tablename__ = "chains"
    __table_args__ = {"comment": "main"}

    chain_id = Column(Integer, primary_key=True)
    shikimori_id = Column(String)
    animego_url = Column(String)
    mal_id = Column(String)
    kinopoisk_id = Column(String)
    imdb_id = Column(String)

    @classmethod
    async def add(cls, **kwargs):
        async with sessions["main"]() as session:
            session.add(cls(**kwargs))
            await session.commit()

    @classmethod
    async def get(cls, **kwargs):
        async with sessions["main"]() as session:
            result = await session.execute(select(cls).filter_by(**kwargs))
            return result.scalars().first()

    @classmethod
    async def get_all(cls, **kwargs):
        async with sessions["main"]() as session:
            result = await session.execute(select(cls).filter_by(**kwargs))
            return result.scalars().all()

    @classmethod
    async def update(cls, id, **kwargs):
        async with sessions["main"]() as session:
            chain = await cls.get(chain_id=id)
            for key, value in kwargs.items():
                setattr(chain, key, value)
            await session.commit()


async def main():
    chains = await AnimeChains.get_all()
    shikimori2animego = {
        chain.shikimori_id: chain.animego_url
        for chain in chains
        if chain.shikimori_id and chain.animego_url
    }
    kinopoisk2shikimori = {
        chain.kinopoisk_id: chain.shikimori_id
        for chain in chains
        if chain.kinopoisk_id and chain.shikimori_id
    }

    with open("./json/shikimori2animego.json", "w", encoding="utf-8") as f:
        dump(shikimori2animego, f, indent=4, ensure_ascii=False, sort_keys=True)

    with open("./json/kinopoisk2shikimori.json", "w", encoding="utf-8") as f:
        dump(kinopoisk2shikimori, f, indent=4, ensure_ascii=False, sort_keys=True)

    with open("./README.md", "w", encoding="utf-8") as f:
        f.write("# AnimeChains\n\n")
        f.write(f"Total chains: {len(chains)}\n")
        f.write(f"Total shikimori2animego chains: {len(shikimori2animego)}\n")
        f.write(f"Total kinopoisk2shikimori chains: {len(kinopoisk2shikimori)}\n")


run(main())
